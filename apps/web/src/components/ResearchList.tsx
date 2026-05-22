import type { ResearchNote } from "../api/types";
import { formatDateTime } from "../utils/format";

export function ResearchList({ notes }: { notes: ResearchNote[] }) {
  return (
    <div className="research-list">
      {notes.map((note) => (
        <article key={note.id} className="research-item">
          <div>
            <span className="badge">{note.sourceType}</span>
            <span className="muted">{note.ticker}</span>
          </div>
          <h3>{note.title}</h3>
          <p>{note.summary}</p>
          <footer>
            <span>{formatDateTime(note.publishedAt)}</span>
            <span>{Math.round(note.confidence * 100)}% confidence</span>
          </footer>
        </article>
      ))}
    </div>
  );
}
