#!/usr/bin/env python3
"""smtp_sink.py — a minimal fake SMTP server for the cdrom e2e suite (F-18).

Usage: smtp_sink.py <port> <outfile>

Speaks just enough SMTP for the API's net/smtp client (which, with STARTTLS
disabled, does EHLO -> MAIL FROM -> RCPT TO -> DATA -> QUIT): it greets,
accepts the conversation, and appends every accepted message to <outfile>
(one "=== MESSAGE ===" block per message) so the test can count messages and
inspect their headers and body. It is plaintext (no TLS) — the e2e API is
configured with CDROM_EMAIL_STARTTLS=false to match.
"""
import socket
import sys
import threading


def main():
    if len(sys.argv) < 3:
        print("usage: smtp_sink.py <port> <outfile>", file=sys.stderr)
        sys.exit(2)
    port = int(sys.argv[1])
    outfile = sys.argv[2]

    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", port))
    srv.listen(8)

    def handle(conn):
        try:
            conn.sendall(b"220 e2e-smtp ready\r\n")
            buf = b""
            in_data = False
            data = b""
            while True:
                chunk = conn.recv(4096)
                if not chunk:
                    break
                buf += chunk
                while b"\r\n" in buf:
                    line, buf = buf.split(b"\r\n", 1)
                    if in_data:
                        if line == b".":
                            in_data = False
                            with open(outfile, "ab") as f:
                                f.write(b"=== MESSAGE ===\n")
                                f.write(data)
                                f.write(b"\n")
                            conn.sendall(b"250 OK: message accepted\r\n")
                        else:
                            data += line + b"\r\n"
                        continue
                    cmd = line.decode("utf-8", "replace").strip().upper()
                    if cmd.startswith("EHLO") or cmd.startswith("HELO"):
                        conn.sendall(b"250 e2e-smtp\r\n")
                    elif cmd.startswith("DATA"):
                        in_data = True
                        data = b""
                        conn.sendall(b"354 End data with <CR><LF>.<CR><LF>\r\n")
                    elif cmd.startswith("QUIT"):
                        conn.sendall(b"221 Bye\r\n")
                        return
                    else:
                        # MAIL FROM / RCPT TO / RSET / anything else: accept.
                        conn.sendall(b"250 OK\r\n")
        except OSError:
            pass
        finally:
            try:
                conn.close()
            except OSError:
                pass

    while True:
        conn, _ = srv.accept()
        threading.Thread(target=handle, args=(conn,), daemon=True).start()


if __name__ == "__main__":
    main()
