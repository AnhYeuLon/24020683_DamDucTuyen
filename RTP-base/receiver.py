import argparse
import socket
import sys

from utils import PacketHeader, compute_checksum

PKT_START = 0
PKT_END = 1
PKT_DATA = 2
PKT_ACK = 3

HEADER_BYTES = 16


class FrameInspector:
    """ Decodes incoming UDP datagrams and validates CRC32 checksums. """

    @staticmethod
    def make_ack(seq_no: int) -> bytes:
        hdr = PacketHeader(type=PKT_ACK, seq_num=seq_no, length=0, checksum=0)
        hdr.checksum = compute_checksum(hdr / b"")
        return bytes(hdr / b"")

    @staticmethod
    def unpack(raw_datagram: bytes):
        if len(raw_datagram) < HEADER_BYTES:
            return None, None
        hdr = PacketHeader(raw_datagram[:HEADER_BYTES])
        if hdr.length < 0 or len(raw_datagram) < HEADER_BYTES + hdr.length:
            return None, None
        chunk = raw_datagram[HEADER_BYTES : HEADER_BYTES + hdr.length]
        wire_crc = hdr.checksum
        hdr.checksum = 0
        if compute_checksum(hdr / chunk) != wire_crc:
            return None, None
        hdr.checksum = wire_crc
        return hdr, chunk


class CumulativeReceiverNode:
    """ Single-connection RTP receiver using cumulative ACKs (Part 2). """

    def __init__(self, bind_host: str, bind_port: int, win_cap: int):
        self.bind_addr = (bind_host, int(bind_port))
        self.win_cap = max(1, int(win_cap))
        self.udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_sock.bind(self.bind_addr)

        self.session_active = False
        self.peer_endpoint = None
        self.next_expected = 0
        self.stash = {}
        self.deferred_fin_seq = None

    def _EmitAck(self, target_addr, ack_seq: int):
        ack_dgram = FrameInspector.make_ack(ack_seq)
        self.udp_sock.sendto(ack_dgram, target_addr)

    def _DrainContiguous(self):
        flushed_any = False
        while self.next_expected in self.stash:
            segment = self.stash.pop(self.next_expected)
            sys.stdout.buffer.write(segment)
            self.next_expected += 1
            flushed_any = True
        if flushed_any:
            sys.stdout.buffer.flush()
            sys.stdout.flush()

    def serve_once(self):
        try:
            while True:
                raw_pkt, sender_addr = self.udp_sock.recvfrom(2048)
                hdr, payload = FrameInspector.unpack(raw_pkt)
                if hdr is None:
                    continue

                if hdr.type == PKT_START:
                    # Ignore START messages if already inside an active connection
                    if self.session_active:
                        continue
                    if hdr.seq_num == 0:
                        self.session_active = True
                        self.peer_endpoint = sender_addr
                        self.next_expected = 1
                        self._EmitAck(sender_addr, self.next_expected)
                    continue

                if not self.session_active or sender_addr != self.peer_endpoint:
                    continue

                # Drop packets outside the receiving window [next_expected, next_expected + window_size)
                if hdr.seq_num >= self.next_expected + self.win_cap:
                    continue

                if hdr.type == PKT_DATA:
                    if hdr.seq_num >= self.next_expected and hdr.seq_num not in self.stash:
                        self.stash[hdr.seq_num] = payload
                    if hdr.seq_num == self.next_expected:
                        self._DrainContiguous()

                    self._EmitAck(sender_addr, self.next_expected)

                    if (
                        self.deferred_fin_seq is not None
                        and self.next_expected == self.deferred_fin_seq
                    ):
                        self._EmitAck(sender_addr, self.next_expected + 1)
                        break

                elif hdr.type == PKT_END:
                    if hdr.seq_num <= self.next_expected:
                        self._DrainContiguous()
                        self._EmitAck(sender_addr, self.next_expected + 1)
                        break
                    else:
                        self.deferred_fin_seq = hdr.seq_num
                        self._EmitAck(sender_addr, self.next_expected)
        finally:
            sys.stdout.buffer.flush()
            sys.stdout.flush()
            self.udp_sock.close()


def receiver(receiver_ip, receiver_port, window_size):
    """Listen on UDP socket and write received stream to sys.stdout."""
    node = CumulativeReceiverNode(receiver_ip, receiver_port, window_size)
    node.serve_once()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "receiver_ip", help="The IP address of the host that receiver is running on"
    )
    parser.add_argument(
        "receiver_port", type=int, help="The port number on which receiver is listening"
    )
    parser.add_argument(
        "window_size", type=int, help="Maximum number of outstanding packets"
    )
    args = parser.parse_args()

    try:
        receiver(args.receiver_ip, args.receiver_port, args.window_size)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
