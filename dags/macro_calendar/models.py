from dataclasses import asdict, dataclass
from datetime import datetime


@dataclass(frozen=True)
class MacroCalendarEvent:
    event_key: str
    source_event_id: str
    indicator_code: str
    indicator_name: str
    category: str
    source: str
    reference_period: str | None
    release_at_utc: datetime
    importance: int
    event_url: str | None
    status: str
    raw_title: str

    def to_dict(self) -> dict:
        data = asdict(self)
        data["release_at_utc"] = self.release_at_utc.isoformat()
        return data
