import { KeyboardEvent, useEffect, useState } from 'react';

type EditableCellProps = {
  value: string;
  onCommit: (value: string) => void;
  disabled?: boolean;
  column?: string;
  options?: string[];
};

export function EditableCell({ column = '', disabled = false, onCommit, options, value }: EditableCellProps) {
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

  const inputType = /date/i.test(column) ? 'date' : /email/i.test(column) ? 'email' : 'text';

  return editing ? (
    options?.length ? (
      <select
        autoFocus
        className="inline-grid-input"
        value={draft}
        onBlur={save}
        onChange={(event) => setDraft(event.target.value)}
        onKeyDown={(event) => {
          if (event.key === 'Enter') save();
          if (event.key === 'Escape') cancel();
        }}
      >
        <option value="">Not set</option>
        {options.map((option) => <option key={option} value={option}>{option}</option>)}
      </select>
    ) : (
      <input
        autoFocus
        className="inline-grid-input"
        type={inputType}
        value={draft}
        onBlur={save}
        onChange={(event) => setDraft(event.target.value)}
        onKeyDown={handleKeyDown}
      />
    )
  ) : (
    <button className="inline-grid-cell" type="button" onDoubleClick={() => setEditing(true)} onClick={() => undefined}>
      {value || 'Not set'}
    </button>
  );
}
