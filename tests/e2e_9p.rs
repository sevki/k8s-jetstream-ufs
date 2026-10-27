//! End-to-end test of the 9P2000.L server over TCP using a hand-rolled client.

use std::{
    io::{Read, Write},
    net::TcpStream,
    process::{Child, Command, Stdio},
    time::Duration,
};

const NOFID: u32 = u32::MAX;

struct Client(TcpStream);

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

struct Server(Child);
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
    }
}

fn start(root: &std::path::Path) -> (Server, Client) {
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let child = Command::new(env!("CARGO_BIN_EXE_k8s-jetstream-ufs"))
        .args(["serve", "--root"])
        .arg(root)
        .args(["--listen", &format!("127.0.0.1:{port}")])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let server = Server(child);
    for _ in 0..100 {
        if let Ok(c) = TcpStream::connect(("127.0.0.1", port)) {
            c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
            return (server, Client(c));
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("server did not start");
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

#[test]
fn read_existing_file_and_write_new_file() {
    let dir = tempdir();
    std::fs::write(dir.join("hello.txt"), "hello from the export").unwrap();
    let (_srv, mut c) = start(&dir);
    handshake(&mut c);

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
    let dir = tempdir();
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
    let dir = tempdir();
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

fn tempdir() -> std::path::PathBuf {
    let p = std::env::temp_dir().join(format!(
        "ufs-e2e-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p.canonicalize().unwrap()
}
