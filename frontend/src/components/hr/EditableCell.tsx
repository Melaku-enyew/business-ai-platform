import { KeyboardEvent, useEffect, useState } from 'react';

type EditableCellProps = {
  value: string;
  onCommit: (value: string) => void;
  disabled?: boolean;
};

export function EditableCell({ disabled = false, onCommit, value }: EditableCellProps) {
  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState(value);

  useEffect(() => {
    setDraft(value);
  }, [value]);

  function save() {
    setEditing(false);
    if (draft !== value) onCommit(draft);
  }

  function cancel() {
    setDraft(value);
    setEditing(false);
  }

  function handleKeyDown(event: KeyboardEvent<HTMLInputElement>) {
    if (event.key === 'Enter') save();
    if (event.key === 'Escape') cancel();
  }

  if (disabled) return <span>{value || 'Not set'}</span>;

  return editing ? (
    <input
      autoFocus
      className="inline-grid-input"
      value={draft}
      onBlur={save}
      onChange={(event) => setDraft(event.target.value)}
      onKeyDown={handleKeyDown}
    />
  ) : (
    <button className="inline-grid-cell" type="button" onDoubleClick={() => setEditing(true)} onClick={() => undefined}>
      {value || 'Not set'}
    </button>
  );
}
