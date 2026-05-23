import { useEffect, useMemo, useState } from 'react';
import { EditableCell } from './EditableCell';

export type EmployeeGridRow = Record<string, string>;

type EmployeeGridProps = {
  canEdit: boolean;
  rows: EmployeeGridRow[];
  onRowsChange: (rows: EmployeeGridRow[]) => void;
};

const preferredColumns = ['employeeId', 'employee_id', 'name', 'employeeName', 'department', 'title', 'status', 'manager', 'email'];
const pageSizeOptions = [10, 25, 50];
const statusOptions = ['active', 'inactive', 'onboarding', 'approved', 'pending approval', 'archived'];
const departmentOptions = ['HR', 'Finance', 'Engineering', 'Operations', 'CRM', 'Analytics'];

export function EmployeeGrid({ canEdit, onRowsChange, rows }: EmployeeGridProps) {
  const [selectedRows, setSelectedRows] = useState<number[]>([]);
  const [expanded, setExpanded] = useState(false);
  const [fullscreen, setFullscreen] = useState(false);
  const [compact, setCompact] = useState(true);
  const [filter, setFilter] = useState('');
  const [sortColumn, setSortColumn] = useState('');
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(10);
  const [gridMessage, setGridMessage] = useState('');
  const columns = useMemo(() => {
    const discovered = Array.from(new Set(rows.flatMap((row) => Object.keys(row)))).filter((column) => column !== 'recordId');
    return [
      ...preferredColumns.filter((column) => discovered.includes(column)),
      ...discovered.filter((column) => !preferredColumns.includes(column))
    ].slice(0, 10);
  }, [rows]);
  const visibleRows = useMemo(() => {
    const query = filter.toLowerCase().trim();
    const source = query ? rows.filter((row) => Object.values(row).some((value) => String(value).toLowerCase().includes(query))) : rows;
    if (!sortColumn) return source;
    return [...source].sort((a, b) => String(a[sortColumn] ?? '').localeCompare(String(b[sortColumn] ?? '')));
  }, [filter, rows, sortColumn]);
  const pageCount = Math.max(1, Math.ceil(visibleRows.length / pageSize));
  const currentPage = Math.min(page, pageCount);
  const pagedRows = visibleRows.slice((currentPage - 1) * pageSize, currentPage * pageSize);

  useEffect(() => {
    setPage(1);
    setSelectedRows([]);
  }, [filter, pageSize, rows.length, sortColumn]);

  function rowKey(row: EmployeeGridRow) {
    return String(row.employeeId || row.employee_id || row.email || row.name || row.employeeName || '').toLowerCase().trim();
  }

  function uniqueFieldKey(row: EmployeeGridRow) {
    return String(row.employeeId || row.employee_id || row.email || '').toLowerCase().trim();
  }

  function optionsForColumn(column: string) {
    if (/status/i.test(column)) return statusOptions;
    if (/department/i.test(column)) return departmentOptions;
    return undefined;
  }

  function updateCell(rowIndex: number, column: string, value: string) {
    onRowsChange(rows.map((row, index) => index === rowIndex ? { ...row, [column]: value } : row));
    setGridMessage(`${column} updated. Changes are saved to the active employee dataset.`);
  }

  function deleteRow(rowIndex: number) {
    onRowsChange(rows.filter((_, index) => index !== rowIndex));
    setSelectedRows((current) => current.filter((index) => index !== rowIndex));
    setGridMessage('Row removed and archived in the HR dataset history.');
  }

  function duplicateRow(rowIndex: number) {
    const row = rows[rowIndex] ?? {};
    onRowsChange([...rows.slice(0, rowIndex + 1), { ...row, employeeId: '', employee_id: '', email: '', status: 'draft' }, ...rows.slice(rowIndex + 1)]);
    setGridMessage('Duplicate row staged as draft with unique fields cleared.');
  }

  function bulkUpdate(column: string, value: string) {
    if (!selectedRows.length) return;
    const selected = new Set(selectedRows);
    onRowsChange(rows.map((row, index) => selected.has(index) ? { ...row, [column]: value } : row));
    setGridMessage(`${selectedRows.length} selected rows updated.`);
  }

  function bulkDelete() {
    if (!selectedRows.length) return;
    const selected = new Set(selectedRows);
    onRowsChange(rows.filter((_, index) => !selected.has(index)));
    setGridMessage(`${selectedRows.length} selected rows removed and archived.`);
    setSelectedRows([]);
  }

  return (
    <section className={`hr-inline-grid ${compact ? 'compact-density' : ''} ${fullscreen ? 'fullscreen-grid' : ''}`}>
      <div className="hr-inline-grid-toolbar">
        <strong>Employee Dataset</strong>
        <span>{rows.length.toLocaleString()} rows. Showing {pagedRows.length.toLocaleString()} on this page.</span>
        <button type="button" onClick={() => setExpanded((open) => !open)}>{expanded ? 'Collapse' : 'Expand'}</button>
        <button type="button" onClick={() => setCompact((dense) => !dense)}>{compact ? 'Comfortable' : 'Compact'}</button>
        <button type="button" onClick={() => setFullscreen((open) => !open)}>{fullscreen ? 'Exit Fullscreen' : 'Fullscreen'}</button>
      </div>
      {expanded && <div className="hr-inline-grid-toolbar secondary">
        <input placeholder="Search rows" value={filter} onChange={(event) => setFilter(event.target.value)} />
        <select value={sortColumn} onChange={(event) => setSortColumn(event.target.value)}>
          <option value="">No sorting</option>
          {columns.map((column) => <option key={column} value={column}>{column}</option>)}
        </select>
        <select value={pageSize} onChange={(event) => setPageSize(Number(event.target.value))}>
          {pageSizeOptions.map((size) => <option key={size} value={size}>{size} rows</option>)}
        </select>
        <button type="button" disabled={!canEdit} onClick={() => onRowsChange([...rows, Object.fromEntries(columns.map((column) => [column, '']))])}>Add row</button>
        {selectedRows.length > 0 && <>
          <button type="button" disabled={!canEdit} onClick={() => bulkUpdate('status', 'approved')}>Bulk approve</button>
          <button type="button" disabled={!canEdit} onClick={() => bulkUpdate('status', 'active')}>Bulk status update</button>
          <button type="button" disabled={!canEdit} onClick={() => bulkUpdate('department', 'HR')}>Bulk department update</button>
          <button className="danger-action" type="button" disabled={!canEdit} onClick={bulkDelete}>Bulk delete</button>
        </>}
      </div>}
      {gridMessage && <p className="persistence-note">{gridMessage}</p>}
      {expanded && <div className="hr-grid-scroll">
        <table>
          <thead>
            <tr>
              <th>Select</th>
              {columns.map((column) => <th key={column}><button type="button" onClick={() => setSortColumn(column)}>{column}</button></th>)}
              <th>Row Actions</th>
            </tr>
          </thead>
          <tbody>
            {pagedRows.map((row) => {
              const sourceIndex = rows.indexOf(row);
              const key = rowKey(row);
              const uniqueKey = uniqueFieldKey(row);
              const duplicate = Boolean(uniqueKey && rows.some((entry, index) => index !== sourceIndex && uniqueFieldKey(entry) === uniqueKey));
              return (
              <tr className={`${selectedRows.includes(sourceIndex) ? 'selected-row' : ''} ${duplicate ? 'invalid-row' : ''}`} key={`${sourceIndex}-${key || 'row'}`}>
                <td>
                  <input
                    checked={selectedRows.includes(sourceIndex)}
                    type="checkbox"
                    onChange={() => setSelectedRows((current) => current.includes(sourceIndex) ? current.filter((index) => index !== sourceIndex) : [...current, sourceIndex])}
                  />
                </td>
                {columns.map((column) => (
                  <td key={column}>
                    <EditableCell column={column} disabled={!canEdit} options={optionsForColumn(column)} value={String(row[column] ?? '')} onCommit={(value) => updateCell(sourceIndex, column, value)} />
                    {duplicate && /employee|email|name/i.test(column) && <small className="inline-validation">Duplicate</small>}
                  </td>
                ))}
                <td>
                  <div className="inline-actions">
                    <button className="ghost-button compact" type="button" disabled={!canEdit} onClick={() => duplicateRow(sourceIndex)}>Duplicate</button>
                    <button className="ghost-button compact danger" type="button" disabled={!canEdit} onClick={() => deleteRow(sourceIndex)}>Delete</button>
                  </div>
                </td>
              </tr>
              );
            })}
            {!rows.length && <tr><td colSpan={columns.length + 2}>No rows in the active employee dataset.</td></tr>}
          </tbody>
        </table>
      </div>}
      {expanded && <div className="hr-grid-pagination">
        <button type="button" disabled={currentPage <= 1} onClick={() => setPage((value) => Math.max(1, value - 1))}>Previous</button>
        <span>Page {currentPage} of {pageCount}</span>
        <button type="button" disabled={currentPage >= pageCount} onClick={() => setPage((value) => Math.min(pageCount, value + 1))}>Next</button>
      </div>}
    </section>
  );
}
