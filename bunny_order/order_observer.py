from watchdog.observers import Observer
from watchdog.events import (
    FileCreatedEvent,
    FileModifiedEvent,
    FileDeletedEvent,
    FileMovedEvent,
    FileSystemEventHandler,
)

from bunny_order.config import Config


class FileEventHandler(FileSystemEventHandler):
    def on_any_event(self, event):
        pass

    def on_moved(self, event: FileMovedEvent):
        if event.is_directory:
            print(
                "directory moved from {0} to {1}".format(
                    event.src_path, event.dest_path
                )
            )
        else:
            print("file moved from {0} to {1}".format(event.src_path, event.dest_path))

    def on_created(self, event: FileCreatedEvent):
        if event.is_directory:
            print("directory created:{0}".format(event.src_path))
        else:
            print("file created:{0}".format(event.src_path))

    def on_deleted(self, event: FileDeletedEvent):
        if event.is_directory:
            print("directory deleted:{0}".format(event.src_path))
        else:
            print("file deleted:{0}".format(event.src_path))

    def on_modified(self, event: FileModifiedEvent):
        if event.is_directory:
            print("directory modified:{0}".format(event.src_path))
        else:
            print("file modified:{0}".format(event.src_path))
            if event.src_path.endswith(Config.OBSERVER_ORDER_FILE):
                print("order")
            elif event.src_path.endswith(Config.OBSERVER_TRADE_FILE):
                print("trade")
            elif event.src_path.endswith(Config.OBSERVER_POSITION_FILE):
                print("position")


class OrderObserver:
    def __init__(self):
        self.observer = Observer()
        self.observer.setDaemon(True)
        self.event_handler = FileEventHandler()
        self.observer.schedule(self.event_handler, Config.OBSERVER_PATH, False)

    def __del__(self):
        self.observer.stop()

    def start(self):
        self.observer.start()
