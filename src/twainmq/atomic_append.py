import os
from pathlib import Path
import errno
import ctypes
from ctypes import wintypes
from pathlib import Path

# Win32 constants
FILE_APPEND_DATA        = 0x0004
FILE_SHARE_READ         = 0x00000001
FILE_SHARE_WRITE        = 0x00000002
OPEN_ALWAYS             = 4
FILE_ATTRIBUTE_NORMAL   = 0x80
INVALID_HANDLE_VALUE    = wintypes.HANDLE(-1).value

kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)

def atomic_append_win(path, data: bytes) -> None:
    """Atomic appending to a file under Windows.
    
    This makes use of the windows FILE_APPEND_DATA mode which doesn't check the file length first or use a cursor
    it simply dumps the data on the end of the file.
    """
    # Accept Path or str
    if isinstance(path, Path):
        path = str(path)

    # 1. Open with FILE_APPEND_DATA only (critical)
    handle = kernel32.CreateFileW(
        wintypes.LPCWSTR(path),
        FILE_APPEND_DATA,                         # <-- atomic append mode
        FILE_SHARE_READ | FILE_SHARE_WRITE,
        None,
        OPEN_ALWAYS,
        FILE_ATTRIBUTE_NORMAL,
        None,
    )

    if handle == INVALID_HANDLE_VALUE:
        raise OSError(ctypes.get_last_error(), "CreateFileW failed")

    try:
        # 2. WriteFile (one syscall, atomic append)
        written = wintypes.DWORD(0)
        ok = kernel32.WriteFile(
            handle,
            data,
            len(data),
            ctypes.byref(written),
            None,
        )

        if not ok or written.value != len(data):
            raise OSError(ctypes.get_last_error(), "WriteFile failed")

    finally:
        kernel32.CloseHandle(handle)

import os
import errno
from pathlib import Path

def atomic_append_posix(path, data: bytes, mode: int = 0o644) -> None:
    """Atomic append function for POSIX systems.

    This should work by using O_APPEND but this is not properly tested yet!

    Short write might in theory happen, but shouldn't, when we can test this better
    we will know if this is a problem and come up with a solution.
    """
    # Accept Path or str
    if isinstance(path, Path):
        path = os.fspath(path)

    # Open with O_APPEND so the kernel moves the file offset atomically
    flags = os.O_WRONLY | os.O_APPEND | os.O_CREAT

    # Add close-on-exec if available (good hygiene)
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC

    fd = os.open(path, flags, mode)
    try:
        # Exactly one write() syscall
        n = os.write(fd, data)

        # POSIX guarantees full writes to regular files unless an error occurs.
        # If we ever see a short write, treat it as fatal.
        if n != len(data):
            raise OSError(errno.EIO, f"short write: wrote {n} of {len(data)} bytes")

    finally:
        os.close(fd)

def atomic_append(path, data: bytes, mode: int = 0o644) -> None:
    """Atomic writing used by producers to allow multiple producers to write to the same 
    log file at the same time without locks"""
    if os.name == "nt":
        atomic_append_win(path, data)
    else:
        atomic_append_posix(path, data)
