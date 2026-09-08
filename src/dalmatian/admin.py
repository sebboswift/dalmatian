from __future__ import annotations

import argparse

from dalmatian.config import get_settings
from dalmatian.metadata import build_metadata_store, current_metadata_revision, upgrade_metadata
from dalmatian.storage import StorageRegistry


def run() -> None:
    parser = argparse.ArgumentParser(prog="dalmatian-admin")
    sub = parser.add_subparsers(dest="command", required=True)

    add = sub.add_parser("storage-add")
    add.add_argument("name")
    add.add_argument("uri")
    sub.add_parser("storage-list")
    delete = sub.add_parser("storage-delete")
    delete.add_argument("name")
    sub.add_parser("metadata-upgrade")
    sub.add_parser("metadata-current")
    args = parser.parse_args()

    settings = get_settings()
    if not settings.metadata_url:
        parser.error("DALMATIAN_METADATA_URL is required for metadata commands")

    if args.command == "metadata-upgrade":
        upgrade_metadata(settings.metadata_url)
        print(current_metadata_revision(settings.metadata_url) or "unknown")
        return
    if args.command == "metadata-current":
        print(current_metadata_revision(settings.metadata_url) or "unmigrated")
        return

    store = build_metadata_store(settings.metadata_url)
    try:
        if args.command == "storage-add":
            StorageRegistry({args.name: args.uri})
            store.put_storage_location(args.name, args.uri)
        elif args.command == "storage-delete":
            store.delete_storage_location(args.name)
        else:
            for name, uri in sorted(store.storage_locations().items()):
                print(f"{name}\t{uri}")
    finally:
        store.close()


if __name__ == "__main__":
    run()
