//! End-to-end tests of the 9P2000.L server over its unix socket, QUIC (mTLS) and iroh, using a
//! hand-rolled 9P client. QUIC and iroh go through the same dialer + proxy the CSI node plugin uses.

use std::{
    io::{Read, Write},
    os::unix::net::UnixStream,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::Arc,
    time::Duration,
};

use k8s_jetstream_ufs::transport::{proxy, ClientTls, Dialer, Target};

const NOFID: u32 = u32::MAX;

struct Client(UnixStream);

fn s(buf: &mut Vec<u8>, v: &str) {
    buf.extend((v.len() as u16).to_le_bytes());
    buf.extend(v.as_bytes());
}

impl Client {
    /// Sends one T-message and returns the R-message body (type checked).
    fn call(&mut self, ty: u8, body: &[u8]) -> Vec<u8> {
        let mut m = ((7 + body.len()) as u32).to_le_bytes().to_vec();
        m.push(ty);
        m.extend(1u16.to_le_bytes());
        m.extend(body);
        self.0.write_all(&m).unwrap();
        let mut hdr = [0u8; 7];
        self.0.read_exact(&mut hdr).unwrap();
        let size = u32::from_le_bytes(hdr[..4].try_into().unwrap()) as usize;
        let mut rest = vec![0u8; size - 7];
        self.0.read_exact(&mut rest).unwrap();
        if hdr[4] == 7 {
            panic!(
                "Rlerror errno={}",
                u32::from_le_bytes(rest[..4].try_into().unwrap())
            );
        }
        assert_eq!(hdr[4], ty + 1, "unexpected reply type");
        rest
    }

    fn walk(&mut self, fid: u32, new: u32, names: &[&str]) {
        let mut b = [fid.to_le_bytes(), new.to_le_bytes()].concat();
        b.extend((names.len() as u16).to_le_bytes());
        for n in names {
            s(&mut b, n);
        }
        self.call(110, &b);
    }

    fn read_all(&mut self, fid: u32) -> Vec<u8> {
        self.call(
            12,
            &[fid.to_le_bytes().as_slice(), &0u32.to_le_bytes()].concat(),
        );
        let mut b = fid.to_le_bytes().to_vec();
        b.extend(0u64.to_le_bytes());
        b.extend(4096u32.to_le_bytes());
        let r = self.call(116, &b);
        let n = u32::from_le_bytes(r[..4].try_into().unwrap()) as usize;
        r[4..4 + n].to_vec()
    }
}

struct Server {
    child: Child,
    socket: PathBuf,
}
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn spawn_server(root: &Path, extra: &[String]) -> Server {
    let socket = root.parent().unwrap().join(format!(
        "{}.sock",
        root.file_name().unwrap().to_string_lossy()
    ));
    let _ = std::fs::remove_file(&socket);
    let child = Command::new(env!("CARGO_BIN_EXE_k8s-jetstream-ufs"))
        .args(["serve", "--root"])
        .arg(root)
        .arg("--socket")
        .arg(&socket)
        .args(extra)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    // Owning the child in a `Server` means it is killed and reaped on every path, panics included.
    let server = Server { child, socket };
    // The unix socket is bound after the QUIC and iroh listeners.
    for _ in 0..200 {
        if UnixStream::connect(&server.socket).is_ok() {
            return server;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("server did not start");
}

fn connect_unix(path: &Path) -> Client {
    let c = UnixStream::connect(path).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(20))).unwrap();
    Client(c)
}

fn free_udp_port() -> u16 {
    std::net::UdpSocket::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn start(root: &Path) -> (Server, Client) {
    let srv = spawn_server(root, &[]);
    let c = connect_unix(&srv.socket);
    (srv, c)
}

/// Runs the node plugin's proxy for `target` on a background runtime and returns a client of it.
fn proxied(dialer: Dialer, target: Target, sock: &Path) -> Client {
    let _ = std::fs::remove_file(sock);
    let sock = sock.to_path_buf();
    let sock2 = sock.clone();
    std::thread::spawn(move || {
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(async move {
                let listener = tokio::net::UnixListener::bind(&sock2).unwrap();
                proxy(listener, Arc::new(dialer), target).await;
            })
    });
    for _ in 0..100 {
        if sock.exists() {
            return connect_unix(&sock);
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("proxy did not start");
}

fn handshake(c: &mut Client) {
    attach(c, "/");
}

fn attach(c: &mut Client, aname: &str) {
    let mut b = 65536u32.to_le_bytes().to_vec();
    s(&mut b, "9P2000.L");
    let r = c.call(100, &b);
    assert!(r.ends_with(b"9P2000.L"), "version negotiation: {r:?}");

    let mut b = [0u32.to_le_bytes(), NOFID.to_le_bytes()].concat();
    s(&mut b, "root");
    s(&mut b, aname);
    b.extend(0u32.to_le_bytes());
    c.call(104, &b);
}

fn tempdir(name: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!("ufs-e2e-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p.canonicalize().unwrap()
}

fn write_certs(dir: &Path) -> (ClientTls, Vec<String>) {
    use rcgen::{BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair};
    let ca_key = KeyPair::generate().unwrap();
    let mut ca_params = CertificateParams::new(vec![]).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca = ca_params.self_signed(&ca_key).unwrap();

    let issue = |name: &str, usage| {
        let key = KeyPair::generate().unwrap();
        let mut p = CertificateParams::new(vec![name.to_string()]).unwrap();
        p.extended_key_usages = vec![usage];
        let cert = p.signed_by(&key, &ca, &ca_key).unwrap();
        (cert.pem(), key.serialize_pem())
    };
    let (srv_crt, srv_key) = issue("localhost", ExtendedKeyUsagePurpose::ServerAuth);
    let (cli_crt, cli_key) = issue("client", ExtendedKeyUsagePurpose::ClientAuth);
    let w = |n: &str, c: &str| {
        let p = dir.join(n);
        std::fs::write(&p, c).unwrap();
        p
    };
    let ca_pem = w("ca.crt", &ca.pem());
    let (sc, sk) = (w("server.crt", &srv_crt), w("server.key", &srv_key));
    let (cc, ck) = (w("client.crt", &cli_crt), w("client.key", &cli_key));
    let tls = ClientTls {
        ca: ca_pem.clone(),
        identity: Some((cc, ck)),
    };
    (
        tls,
        vec![
            "--tls-cert".into(),
            sc.display().to_string(),
            "--tls-key".into(),
            sk.display().to_string(),
            "--client-ca".into(),
            ca_pem.display().to_string(),
        ],
    )
}

#[test]
fn read_existing_file_and_write_new_file() {
    let dir = tempdir("rw");
    std::fs::write(dir.join("hello.txt"), "hello from the export").unwrap();
    let (_srv, mut c) = start(&dir);
    handshake(&mut c);
    check_rw(&mut c, &dir);
}

fn check_rw(c: &mut Client, dir: &Path) {
    // read an existing file
    c.walk(0, 1, &["hello.txt"]);
    assert_eq!(c.read_all(1), b"hello from the export");

    // create + write a new file
    c.walk(0, 2, &[]);
    let mut b = 2u32.to_le_bytes().to_vec();
    s(&mut b, "new.txt");
    b.extend(0o102u32.to_le_bytes()); // O_RDWR|O_CREAT
    b.extend(0o644u32.to_le_bytes());
    b.extend(0u32.to_le_bytes());
    c.call(14, &b);
    let data = b"written over 9p";
    let mut b = 2u32.to_le_bytes().to_vec();
    b.extend(0u64.to_le_bytes());
    b.extend((data.len() as u32).to_le_bytes());
    b.extend(data);
    c.call(118, &b);
    c.call(120, &2u32.to_le_bytes()); // clunk

    assert_eq!(std::fs::read(dir.join("new.txt")).unwrap(), data);
}

#[test]
fn attach_name_selects_subdirectory() {
    let dir = tempdir("subdir");
    std::fs::create_dir_all(dir.join("vol-a")).unwrap();
    std::fs::write(dir.join("vol-a/f.txt"), "in a").unwrap();
    std::fs::write(dir.join("top.txt"), "top").unwrap();
    let (_srv, mut c) = start(&dir);
    let mut b = 65536u32.to_le_bytes().to_vec();
    s(&mut b, "9P2000.L");
    c.call(100, &b);
    attach(&mut c, "vol-a");
    c.walk(0, 1, &["f.txt"]);
    assert_eq!(c.read_all(1), b"in a");
}

#[test]
fn attach_rejects_escape_and_missing() {
    let dir = tempdir("escape");
    let (_srv, mut c) = start(&dir);
    let mut b = 65536u32.to_le_bytes().to_vec();
    s(&mut b, "9P2000.L");
    c.call(100, &b);
    for aname in ["../etc", "nope"] {
        let mut b = [0u32.to_le_bytes(), NOFID.to_le_bytes()].concat();
        s(&mut b, "root");
        s(&mut b, aname);
        b.extend(0u32.to_le_bytes());
        let mut m = ((7 + b.len()) as u32).to_le_bytes().to_vec();
        m.push(104);
        m.extend(1u16.to_le_bytes());
        m.extend(&b);
        c.0.write_all(&m).unwrap();
        let mut hdr = [0u8; 7];
        c.0.read_exact(&mut hdr).unwrap();
        assert_eq!(hdr[4], 7, "{aname} should get Rlerror");
        let mut rest = [0u8; 4];
        c.0.read_exact(&mut rest).unwrap();
    }
}

#[test]
fn quic_mtls_through_proxy() {
    let dir = tempdir("quic");
    std::fs::write(dir.join("hello.txt"), "hello from the export").unwrap();
    let certs = tempdir("quic-certs");
    let (tls, mut args) = write_certs(&certs);
    let port = free_udp_port();
    args.extend(["--quic-listen".into(), format!("127.0.0.1:{port}")]);
    let _srv = spawn_server(&dir, &args);

    let target = Target::Quic {
        host: "localhost".into(),
        port,
    };
    let mut c = proxied(
        Dialer::new(Some(tls.clone())),
        target.clone(),
        &certs.join("p.sock"),
    );
    handshake(&mut c);
    check_rw(&mut c, &dir);

    // Without a client certificate the server must refuse the connection.
    let anon = ClientTls {
        identity: None,
        ..tls
    };
    let mut c = proxied(Dialer::new(Some(anon)), target, &certs.join("anon.sock"));
    c.0.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    let mut b = 65536u32.to_le_bytes().to_vec();
    s(&mut b, "9P2000.L");
    let mut m = ((7 + b.len()) as u32).to_le_bytes().to_vec();
    m.push(100);
    m.extend(1u16.to_le_bytes());
    m.extend(&b);
    c.0.write_all(&m).unwrap();
    let mut hdr = [0u8; 7];
    assert!(
        c.0.read_exact(&mut hdr).is_err(),
        "server answered a client with no certificate"
    );
}

#[test]
fn iroh_through_proxy() {
    let dir = tempdir("iroh");
    std::fs::write(dir.join("hello.txt"), "hello from the export").unwrap();
    let key = dir.parent().unwrap().join(format!(
        "{}-iroh.key",
        dir.file_name().unwrap().to_string_lossy()
    ));
    let _ = std::fs::remove_file(&key);
    let out = Command::new(env!("CARGO_BIN_EXE_k8s-jetstream-ufs"))
        .args(["iroh-keygen", "--out"])
        .arg(&key)
        .output()
        .unwrap();
    assert!(out.status.success());
    let node = String::from_utf8(out.stdout).unwrap().trim().to_string();

    let port = free_udp_port();
    let _srv = spawn_server(
        &dir,
        &[
            "--iroh-listen".into(),
            format!("127.0.0.1:{port}"),
            "--iroh-secret-file".into(),
            key.display().to_string(),
        ],
    );
    let target = Target::Iroh {
        node,
        addrs: vec![format!("127.0.0.1:{port}")],
    };
    let mut c = proxied(
        Dialer::new(None),
        target,
        &dir.parent().unwrap().join("iroh-proxy.sock"),
    );
    handshake(&mut c);
    check_rw(&mut c, &dir);
}
