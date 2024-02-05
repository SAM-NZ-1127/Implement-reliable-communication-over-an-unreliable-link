"""
Shivam Jayeshkumar Mehta
Computer Networks - Homework 4

This program implements a TCP-like reliable data transfer protocol over an unreliable .
"""

import socket
import io
import time
import typing
import struct
import homework4 # NOTE: file names are changed  as per homework title to run the program.
import homework4.logging # NOTE: file names are changed as per homework title to run the program.

# Constants
INITIAL_TIMEOUT = 0.5
packet_header = 4 #header size = 4 bytes (sequence number is an integer)
MAX_PACKET_SIZE = homework4.MAX_PACKET - packet_header  # Reserve 4 bytes for the header (sequence number = integer)

# function to calculate new timeout
def calculate_timeout(estimated_rtt, dev_rtt, alpha=0.125, beta=0.25):
    # Using TCP's default  values for alpha and beta
    return estimated_rtt + 4 * dev_rtt

def send(sock: socket.socket, data: bytes):
    """
    Send data in chunks over a socket, implementing retries for lost packets.
    """
    logger = homework4.logging.get_logger("hw4-sender")
    sequence_number = 0  # Initialize sequence number
    estimated_rtt = INITIAL_TIMEOUT  # Initialize estimated RTT
    dev_rtt = 0  # Initialize RTT deviation
    # Split data into chunks based on MAX_PACKET_SIZE
    offsets = range(0, len(data), MAX_PACKET_SIZE)
    data_chunks = [data[i:i + MAX_PACKET_SIZE] for i in offsets]
    # TCP's default values for alpha and beta
    alpha = 0.125
    beta = 0.25
    for chunk in data_chunks:
        packet = struct.pack('!I', sequence_number) + chunk  # Construct packet with sequence number
        while True:
            try:
                start_time = time.time()
                sock.send(packet)
                sock.settimeout(calculate_timeout(estimated_rtt, dev_rtt))  # Set dynamic timeout
                ack = sock.recv(4)  # Wait for ACK
                end_time = time.time()
                ack_sequence_number = struct.unpack('!I', ack)[0]
                if ack_sequence_number == sequence_number:
                    # Calculate RTT and update estimates
                    sample_rtt = end_time - start_time
                    dev_rtt = (1 - beta) * dev_rtt + beta * abs(sample_rtt - estimated_rtt) # Calculating EWMA
                    estimated_rtt = (1 - alpha) * estimated_rtt + alpha * sample_rtt
                    break  # Correct ACK received; proceed to next chunk
            except socket.timeout:
                # Timeout occurred, packet will be resent
                continue
            finally:
                # Clear timeout after each send attempt
                sock.settimeout(None)
        sequence_number += 1  # Increment sequence number for next packet
    fin_packet = struct.pack('!I', sequence_number)  # Construct FIN packet
    sock.send(fin_packet)  # Send FIN packet to indicate end of transmission

def recv(sock: socket.socket, destination: io.BufferedIOBase) -> int:
    """
    Receive data chunks over a socket and write them to the destination buffer.
    """
    logger = homework4.logging.get_logger("hw4-receiver")
    expected_sequence_num = 0  # Expected sequence number
    received_bytes = 0  # Counter for received bytes
    buffer = {}  # Buffer for out-of-order packets

    while True:
        packet = sock.recv(homework4.MAX_PACKET)  # Receive packet
        if not packet:
            break  # No more data to receive, exit loop
        sequence_num, = struct.unpack('!I', packet[:4])  # Extract sequence number
        if sequence_num == expected_sequence_num:
            # Sequence number is what we expect, write data to destination
            data_chunk = packet[4:]
            destination.write(data_chunk)
            destination.flush()
            received_bytes += len(data_chunk)
            expected_sequence_num += 1  # Increment expected sequence number
            # Write any buffered packets that now fit the sequence
            while expected_sequence_num in buffer:
                destination.write(buffer.pop(expected_sequence_num))
                expected_sequence_num += 1
        else:
            # Buffer out-of-order packets
            buffer[sequence_num] = packet[4:]
        ack_packet = struct.pack('!I', sequence_num)  # Construct ACK packet
        sock.send(ack_packet)  # Send ACK for received packet
    return received_bytes  # Return total number of bytes received