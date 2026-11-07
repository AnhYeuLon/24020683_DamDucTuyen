# Project 4: Reliable Transport (RTP)

## Author and collaborators
### Author name
Dam Duc Tuyen - 24020683

### Collaborators
None

## Report
- **Part 1 (`RTP-base/sender.py`)**: Implemented sliding-window reliable UDP transport (`CumulativeSenderSession`) with 1456-byte maximum data payloads (1472-byte max UDP datagram including 16-byte `PacketHeader`), CRC32 checksum generation/verification, `START` handshake (`seq_num=0`), cumulative ACK window advancement, 500ms retransmission timer for all unacknowledged packets in the active window, and `END` connection teardown (`seq_num=N+1`) with a 500ms wait timer.
- **Part 2 (`RTP-base/receiver.py`)**: Implemented single-sender UDP receiver (`CumulativeReceiverNode`) that ignores additional `START` packets during an active connection, verifies CRC32 checksums, drops packets with `seq_num >= expected_seq + window_size`, buffers out-of-order packets within the window, flushes contiguous in-order data to `sys.stdout`, and sends cumulative ACKs (`seq_num = expected_seq`).
- **Part 3 (`RTP-opt/sender.py` & `RTP-opt/receiver.py`)**: Implemented selective acknowledgment optimization (`SelectiveSenderSession` and `SelectiveReceiverNode`). The receiver sends individual ACKs matching the received `DATA` packet's `seq_num`, and the sender tracks individually acknowledged sequence numbers in the current window so that when the 500ms window timer expires, only unacknowledged packets in the window are retransmitted.

## Citations
- Python `socket` standard library documentation (`SOCK_DGRAM`, `sendto`, `recvfrom`, `settimeout`).

## Grading notes (if any)
Passed all proxy tests (`error_types = 0123`: jam, delay, reorder, drop) for both `RTP-base` and `RTP-opt`.

## Extra credit attempted (if any)
None
