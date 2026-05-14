import logging

logger = logging.getLogger(__name__)


class DeduplicationService:
    """
    Removes duplicate records within a scan.
    Salesforce can return duplicates across pages.
    """

    def deduplicate(self, records: list, id_field: str = "Id") -> list:
        """
        Deduplicates on Salesforce record Id.
        Keeps the last occurrence of each Id.
        """
        if not records:
            return []

        seen = {}
        for record in records:
            record_id = record.get(id_field)
            if record_id:
                seen[record_id] = record
            else:
                # No Id field — keep as is
                seen[id(record)] = record

        original_count = len(records)
        deduped = list(seen.values())
        removed = original_count - len(deduped)

        if removed > 0:
            logger.info(f"Deduplication removed {removed} duplicates from {original_count} records")

        return deduped