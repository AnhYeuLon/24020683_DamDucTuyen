import argparse
import socket
import sys
import time

from utils import PacketHeader, compute_checksum

PKT_START = 0
PKT_END = 1
PKT_DATA = 2
PKT_ACK = 3

HEADER_BYTES = 16
MAX_DGRAM_BYTES = 1472
CHUNK_CAPACITY = MAX_DGRAM_BYTES - HEADER_BYTES
RETRANSMIT_INTERVAL = 0.5


class FrameCodec:
    """Helper for encoding and validating RTP UDP frames."""

    @staticmethod
    def build(frame_type: int, seq_no: int, body: bytes = b"") -> bytes:
        hdr = PacketHeader(type=frame_type, seq_num=seq_no, length=len(body), checksum=0)
        hdr.checksum = compute_checksum(hdr / body)
        return bytes(hdr / body)

    @staticmethod
    def decode(raw_bytes: bytes):
        if len(raw_bytes) < HEADER_BYTES:
            return None, None
        hdr = PacketHeader(raw_bytes[:HEADER_BYTES])
        if hdr.length < 0 or len(raw_bytes) < HEADER_BYTES + hdr.length:
            return None, None
        body = raw_bytes[HEADER_BYTES : HEADER_BYTES + hdr.length]
        claimed_crc = hdr.checksum
        hdr.checksum = 0
        if compute_checksum(hdr / body) != claimed_crc:
            return None, None
        hdr.checksum = claimed_crc
        return hdr, body


class CumulativeSenderSession:
    """Sliding-window RTP sender using cumulative acknowledgements (Part 1)."""

    def __init__(self, host: str, port: int, win_cap: int):
        self.endpoint = (host, int(port))
        self.win_cap = max(1, int(win_cap))
        self.udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def _SliceStream(self, raw_stream: bytes):
        segments = []
        for offset in range(0, len(raw_stream), CHUNK_CAPACITY):
            piece = raw_stream[offset : offset + CHUNK_CAPACITY]
            seq_id = len(segments) + 1
            segments.append(FrameCodec.build(PKT_DATA, seq_id, piece))
        return segments

    def _AwaitAck(self, deadline: float):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return None
        self.udp_sock.settimeout(remaining)
        try:
            raw_pkt, _ = self.udp_sock.recvfrom(2048)
        except socket.timeout:
            return None
        except OSError:
            return None
        hdr, _ = FrameCodec.decode(raw_pkt)
        if hdr is None or hdr.type != PKT_ACK:
            return -1
        return hdr.seq_num

    def _HandshakeStart(self):
        syn_frame = FrameCodec.build(PKT_START, 0)
        while True:
            self.udp_sock.sendto(syn_frame, self.endpoint)
            cutoff = time.monotonic() + RETRANSMIT_INTERVAL
            while time.monotonic() < cutoff:
                ack_no = self._AwaitAck(cutoff)
                if ack_no is None:
                    break
                if ack_no >= 1:
                    return

    def _TransmitPayloads(self, encoded_frames):
        total_frames = len(encoded_frames)
        if total_frames == 0:
            return

        left_edge = 1
        cursor = 1
        timer_anchor = time.monotonic()

        while left_edge <= total_frames:
            window_limit = min(total_frames + 1, left_edge + self.win_cap)
            while cursor < window_limit:
                self.udp_sock.sendto(encoded_frames[cursor - 1], self.endpoint)
                cursor += 1

            ack_no = self._AwaitAck(timer_anchor + RETRANSMIT_INTERVAL)
            if ack_no is None:
                # Timeout reached: retransmit all unacknowledged frames in current window
                for seq_id in range(left_edge, cursor):
                    self.udp_sock.sendto(encoded_frames[seq_id - 1], self.endpoint)
                timer_anchor = time.monotonic()
            elif ack_no > left_edge:
                left_edge = min(ack_no, total_frames + 1)
                new_limit = min(total_frames + 1, left_edge + self.win_cap)
                while cursor < new_limit:
                    self.udp_sock.sendto(encoded_frames[cursor - 1], self.endpoint)
                    cursor += 1
                timer_anchor = time.monotonic()

    def _HandshakeEnd(self, final_seq: int):
        fin_frame = FrameCodec.build(PKT_END, final_seq)
        self.udp_sock.sendto(fin_frame, self.endpoint)
        cutoff = time.monotonic() + RETRANSMIT_INTERVAL
        while time.monotonic() < cutoff:
            ack_no = self._AwaitAck(cutoff)
            if ack_no is None:
                break
            if ack_no >= final_seq + 1:
                break

    def execute(self, payload_bytes: bytes):
        try:
            frames = self._SliceStream(payload_bytes)
            self._HandshakeStart()
            self._TransmitPayloads(frames)
            self._HandshakeEnd(len(frames) + 1)
        finally:
            self.udp_sock.close()


def sender(receiver_ip, receiver_port, window_size):
    """Open UDP socket and reliably transmit message from stdin."""
    stream_data = sys.stdin.buffer.read()
    session = CumulativeSenderSession(receiver_ip, receiver_port, window_size)
    session.execute(stream_data)


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
        sender(args.receiver_ip, args.receiver_port, args.window_size)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
