#!/usr/bin/python3

# Import Modules
import ftplib
import os
import sys
import threading
import time
import argparse
import queue
from threading import Thread
from os import listdir
from os.path import isfile, join
import rich.progress as prg

DEBUG_MODE = False


def debug(prompt: str):
    if DEBUG_MODE:
        print(prompt)


class FtpUploadTracker:
    def __init__(self, progress_bar: prg.Progress, progress_task):
        self.bar = progress_bar
        self.task = progress_task

    def handle(self, block):
        self.bar.update(self.task, advance=1024)


class AutoUploadModule:
    def __init__(self, source_dir: str, server: str, progress_bar: prg.Progress, num_threads: int = 3, **kwargs):
        self.source_dir = source_dir
        self.server = server
        self.user = kwargs.get("user")
        self.passwd = kwargs.get("password")
        self.rm = kwargs.get("remove")
        self.mv = kwargs.get("move_to")
        self.remote_dir = kwargs.get("remote_dir")
        self.queue = queue.Queue(maxsize=15)
        self.progress = progress_bar
        self.num_threads = num_threads
        self.workers = [threading.Thread(target=self._worker_thread, args=(n,), daemon=True)
                        for n in range(num_threads)]
        self.files = []
        self.run = False

    def start(self):
        debug(f"Starting")

        self.run = True

        t = Thread(target=self._scanner_thread, daemon=True)
        self.workers.append(t)

        for w in self.workers:
            w.start()

    def stop(self):
        debug(f"Stopping")

        self.run = False
        for w in self.workers:
            w.join()

    def _upload_file(self, file_path: str, remote_name: str, prg_task: prg.TaskID) -> bool:

        debug(f"Uploading {file_path}, {remote_name}, {prg_task}")

        _ret = False

        tracker = FtpUploadTracker(self.progress, prg_task)

        self.progress.refresh()

        ftp = ftplib.FTP()
        try:
            ftp = ftplib.FTP(host=self.server, user=self.user, passwd=self.passwd)
        except ftplib.Error as e:
            print(f"Error connecting to {self.server}: {e}", file=sys.stderr)
            return False

        # force UTF-8 encoding
        ftp.encoding = "utf-8"

        try:
            ftp.cwd(self.remote_dir)
        except ftplib.Error as e:
            print(f"Error: {e}", file=sys.stderr)
            ftp.quit()
            return False

        try:
            with open(file_path, "rb") as f:
                # Command for Uploading the file "STOR filename"
                try:
                    self.progress.start_task(prg_task)
                    ftp.storbinary(f"STOR {remote_name}", f, 1024, tracker.handle)
                    _ret = True
                except ftplib.Error as e:
                    print(f"Error storing file {remote_name}: {e}")
        except FileNotFoundError:
            print(f"{remote_name} - File not found")
        except PermissionError:
            print(f"{remote_name} - File in use")

        ftp.quit()
        del ftp
        del tracker

        if not _ret:
            return False

        return True

    def _move_file(self, file_path, file_name, prg_task: prg.TaskID):
        debug(f"Moving file {file_path} to {self.mv}")
        # self.progress.stop_task(prg_task)
        _new_path = os.path.join(self.mv, file_name)
        os.rename(file_path, _new_path)
        self.progress.remove_task(prg_task)

    def _delete_file(self, file_path, prg_task: prg.TaskID):
        debug(f"Deleting file {file_path}")
        # self.progress.stop_task(prg_task)
        os.remove(file_path)
        self.progress.remove_task(prg_task)

    def _append_file(self, file_path):
        if file_path not in self.files:
            if os.path.isfile(file_path):
                try:
                    # Try to put it to the queue
                    self.queue.put(file_path, block=False)
                    self.files.append(file_path)
                    debug(f"  {file_path} enqueued")
                except queue.Full:
                    # Enter holding pattern if queue is full
                    debug(f"  {file_path} queue is full")
                    time.sleep(5.0)

    def _process_file(self, file_path: str) -> bool:
        # Extract file name from path
        file_name = os.path.basename(file_path)
        # Check again if file still exists
        if not os.path.isfile(file_path):
            print(f"{file_name} has been removed", file=sys.stderr)
            return False
        # Get the file size
        file_size = int(os.path.getsize(file_path))
        # Prevent processing an empty file
        if file_size == 0:
            print(f"{file_name} zero file size", file=sys.stderr)
            return False
        # Create a progress task
        prog_task = self.progress.add_task(file_name, total=file_size, start=False)
        # Now start upload, catch all exceptions
        try:
            if self._upload_file(file_path, file_name, prog_task):
                if self.mv:
                    self._move_file(file_path, file_name, prog_task)
                elif self.rm:
                    self._delete_file(file_path, prog_task)
                print(f"{file_name}")
                return True
        except PermissionError:
            print(f"{file_name} cannot be accessed", file=sys.stderr)
        except Exception as e:
            print(f"{file_name} - Error: {e}", file=sys.stderr)
        # Remove the progress indicator
        self.progress.remove_task(prog_task)

        return False

    def _worker_thread(self, id: int):
        debug(f"Worker thread id {id} started")
        while self.run:
            file_path = None
            # Try to get enqueued filename...
            try:
                file_path = self.queue.get(False)
            except queue.Empty:
                time.sleep(1.0)

            if file_path:
                debug(f"Worker thread id {id} picked file {file_path}")
                try:
                    self._process_file(file_path)
                    # File needs to stay in files list if not (re)moved or it will be enqueued again.
                    if self.rm or self.mv:
                        self.files.remove(file_path)
                except ValueError:
                    debug(f"Worker thread id {id} item not in list {file_path}")
                except Exception as e:
                    print(f"{os.path.basename(file_path)} - Error: {e}", file=sys.stderr)
                finally:
                    self.queue.task_done()
                    debug(f"Worker thread id {id} done {file_path}")

        debug(f"Worker thread id {id} ended")

    def _scanner_thread(self):
        debug(f"Scanner thread started")
        while self.run:
            time.sleep(1)
            files = [f for f in listdir(self.source_dir) if isfile(join(self.source_dir, f))]
            for file in files:
                source_path = os.path.join(self.source_dir, file)
                if self.mv:
                    # In MOVE-mode we have to make sure the file does not exist in target dir
                    target_path = os.path.join(self.mv, file)
                    if os.path.isfile(target_path):
                        print(f"{file} already in {self.mv}")
                        continue
                # Enqueue
                self._append_file(source_path)
        debug("Scanner thread ended")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="FTP Upload daemon")
    parser.add_argument("source", help="Source directory with the files to upload")
    parser.add_argument("ftp_server", help="Server address")
    parser.add_argument("--threads", "-t", help="Number of parallel threads", default=3, type=int)
    parser.add_argument("--ftp_user", "-u", default="anonymous", help="FTP username")
    parser.add_argument("--ftp_password", "-p", default="", help="FTP password")
    parser.add_argument("--ftp_port", "-n", type=int, default=22, help="FTP port")
    parser.add_argument("--ftp_dir", "-d", default="/", help="FTP directory")
    parser.add_argument("--move", "-m", default="", help="Move uploaded files to directory")
    parser.add_argument("--remove", "-r", action="store_true",
                        help="Delete files after uploading", default=False)
    parser.add_argument("--debug", action="store_true")

    args = parser.parse_args()

    source_directory = args.source
    ftp_server = args.ftp_server
    ftp_user = args.ftp_user
    ftp_password = args.ftp_password
    ftp_dir = args.ftp_dir
    delete = args.remove
    move_dir = args.move

    DEBUG_MODE = args.debug

    if not os.path.isabs(source_directory):
        source_directory = os.path.abspath(source_directory)
    if not os.path.isdir(source_directory):
        print(f"Directory not found: '{source_directory}'")
        sys.exit(1)

    if move_dir:
        if delete:
            print(f"--remove [-r] and --move [-m] cannot be used together")
            sys.exit(3)
        if not os.path.isabs(move_dir):
            move_dir = os.path.abspath(move_dir)
        if not os.path.isdir(move_dir):
            print(f"Directory not found: '{move_dir}'")
            sys.exit(2)

    progress = prg.Progress(
        prg.TextColumn("[progress.description]{task.description}"),
        prg.TaskProgressColumn(),
        prg.BarColumn(),
        prg.TotalFileSizeColumn(),
        prg.TransferSpeedColumn(),
        prg.TimeRemainingColumn(),
    )

    debug("Debug mode on")

    with progress as p:

        uploader = AutoUploadModule(source_directory, ftp_server, num_threads=3, user=ftp_user, password=ftp_password,
                                    remote_dir=args.ftp_dir, remove=delete, move_to=move_dir, progress_bar=p)

        uploader.start()

        try:
            while True:
                time.sleep(1.0)
        except KeyboardInterrupt:
            uploader.stop()

    print("Exiting")
