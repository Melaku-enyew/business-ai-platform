import { useMemo, useState } from 'react';
import { EditableCell } from './EditableCell';

export type EmployeeGridRow = Record<string, string>;

type EmployeeGridProps = {
  canEdit: boolean;
  rows: EmployeeGridRow[];
  onRowsChange: (rows: EmployeeGridRow[]) => void;
};

const preferredColumns = ['employeeId', 'employee_id', 'name', 'employeeName', 'department', 'title', 'status', 'manager', 'email'];

export function EmployeeGrid({ canEdit, onRowsChange, rows }: EmployeeGridProps) {
  const [selectedRows, setSelectedRows] = useState<number[]>([]);
  const columns = useMemo(() => {
    const discovered = Array.from(new Set(rows.flatMap((row) => Object.keys(row))));
    return [
      ...preferredColumns.filter((column) => discovered.includes(column)),
      ...discovered.filter((column) => !preferredColumns.includes(column))
    ].slice(0, 10);
  }, [rows]);

  function updateCell(rowIndex: number, column: string, value: string) {
    onRowsChange(rows.map((row, index) => index === rowIndex ? { ...row, [column]: value } : row));
  }

  function deleteRow(rowIndex: number) {
    onRowsChange(rows.filter((_, index) => index !== rowIndex));
    setSelectedRows((current) => current.filter((index) => index !== rowIndex));
  }

  function duplicateRow(rowIndex: number) {
    const row = rows[rowIndex] ?? {};
    onRowsChange([...rows.slice(0, rowIndex + 1), { ...row }, ...rows.slice(rowIndex + 1)]);
  }

  function bulkUpdate(column: string, value: string) {
    if (!selectedRows.length) return;
    const selected = new Set(selectedRows);
    onRowsChange(rows.map((row, index) => selected.has(index) ? { ...row, [column]: value } : row));
  }

  return (
    <section className="hr-inline-grid">
      <div className="hr-inline-grid-toolbar">
        <strong>{rows.length.toLocaleString()} employee rows</strong>
        <span>Double click a cell to edit. Enter saves. Escape cancels.</span>
        <button type="button" disabled={!canEdit} onClick={() => onRowsChange([...rows, Object.fromEntries(columns.map((column) => [column, '']))])}>Add row</button>
        <button type="button" disabled={!canEdit || !selectedRows.length} onClick={() => bulkUpdate('status', 'approved')}>Bulk approve</button>
        <button type="button" disabled={!canEdit || !selectedRows.length} onClick={() => bulkUpdate('status', 'active')}>Bulk status update</button>
        <button type="button" disabled={!canEdit || !selectedRows.length} onClick={() => bulkUpdate('department', 'HR')}>Bulk department update</button>
      </div>
      <div className="hr-grid-scroll">
        <table>
          <thead>
            <tr>
              <th>Select</th>
              {columns.map((column) => <th key={column}>{column}</th>)}
              <th>Row Actions</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row, rowIndex) => (
              <tr className={selectedRows.includes(rowIndex) ? 'selected-row' : ''} key={rowIndex}>
                <td>
                  <input
                    checked={selectedRows.includes(rowIndex)}
                    type="checkbox"
                    onChange={() => setSelectedRows((current) => current.includes(rowIndex) ? current.filter((index) => index !== rowIndex) : [...current, rowIndex])}
                  />
                </td>
                {columns.map((column) => (
                  <td key={column}>
                    <EditableCell disabled={!canEdit} value={String(row[column] ?? '')} onCommit={(value) => updateCell(rowIndex, column, value)} />
                  </td>
                ))}
                <td>
                  <div className="inline-actions">
                    <button className="ghost-button compact" type="button" disabled={!canEdit} onClick={() => duplicateRow(rowIndex)}>Duplicate</button>
                    <button className="ghost-button compact danger" type="button" disabled={!canEdit} onClick={() => deleteRow(rowIndex)}>Delete</button>
                  </div>
                </td>
              </tr>
            ))}
            {!rows.length && <tr><td colSpan={columns.length + 2}>No rows in the active employee dataset.</td></tr>}
          </tbody>
        </table>
      </div>
    </section>
  );
}
