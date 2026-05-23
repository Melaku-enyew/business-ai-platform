import { useMemo, useState } from 'react';
import { EditableCell } from './EditableCell';

export type UploadStep = 'upload' | 'preview' | 'action' | 'validate' | 'edit' | 'approve' | 'process' | 'publish';

type UploadCenterProps = {
  action: string;
  datasetType: string;
  disabledPublish: boolean;
  fileName?: string;
  onActionChange: (value: string) => void;
  onDatasetTypeChange: (value: string) => void;
  onFile: (file: File | null) => void;
  onPublish: () => void;
  onStep: (step: UploadStep) => void;
  step: UploadStep;
};

const datasetTypes = ['Employee', 'Payroll', 'Timesheet', 'PTO', 'Benefits', 'Hiring', 'Performance'];
const uploadActions = ['Create new dataset', 'Merge into existing', 'Append rows', 'Replace dataset'];
const steps: Array<[UploadStep, string]> = [
  ['upload', '1 Upload'],
  ['preview', '2 Preview'],
  ['action', '3 Choose Action'],
  ['validate', '4 Validate'],
  ['edit', '5 Edit'],
  ['approve', '6 Approve'],
  ['process', '7 Process'],
  ['publish', '8 Publish']
];
const stepOrder = steps.map(([key]) => key);

type ImportRow = Record<string, string>;

function parseCsv(text: string): ImportRow[] {
  const lines = text.split(/\r?\n/).filter((line) => line.trim());
  const headers = splitCsvLine(lines[0] ?? '').map((header) => header.trim() || 'column');
  return lines.slice(1).map((line) => {
    const values = splitCsvLine(line);
    return Object.fromEntries(headers.map((header, index) => [header, values[index] ?? '']));
  });
}

function splitCsvLine(line: string) {
  const values: string[] = [];
  let current = '';
  let quoted = false;
  for (let index = 0; index < line.length; index += 1) {
    const char = line[index];
    if (char === '"') {
      quoted = !quoted;
    } else if (char === ',' && !quoted) {
      values.push(current.trim().replace(/^"|"$/g, ''));
      current = '';
    } else {
      current += char;
    }
  }
  values.push(current.trim().replace(/^"|"$/g, ''));
  return values;
}

async function parseImportFile(file: File) {
  const extension = file.name.split('.').pop()?.toLowerCase();
  const text = await file.text();
  if (extension === 'json') {
    const parsed = JSON.parse(text);
    const rows = Array.isArray(parsed) ? parsed : Array.isArray(parsed.rows) ? parsed.rows : [parsed];
    return rows.map((row) => Object.fromEntries(Object.entries(row ?? {}).map(([key, value]) => [key, String(value ?? '')])));
  }
  if (extension === 'csv' || file.type.includes('csv') || text.includes(',')) return parseCsv(text);
  return [];
}

function rowWarnings(rows: ImportRow[], datasetType: string) {
  const headers = Array.from(new Set(rows.flatMap((row) => Object.keys(row))));
  const lowered = headers.map((header) => header.toLowerCase());
  const warnings: string[] = [];
  const hasEmployeeId = lowered.some((header) => ['employeeid', 'employee_id', 'employee id'].includes(header));
  if (['Employee', 'Payroll', 'Timesheet', 'PTO'].includes(datasetType) && !hasEmployeeId) warnings.push('Missing employeeId');
  if (rows.some((row) => Object.values(row).some((value) => value.trim() === ''))) warnings.push('Empty required value');
  if (rows.some((row) => Object.entries(row).some(([key, value]) => /date/i.test(key) && value && Number.isNaN(new Date(value).getTime())))) warnings.push('Invalid hireDate or workDate');
  if (rows.some((row) => Object.entries(row).some(([key, value]) => /hour/i.test(key) && value && Number.isNaN(Number(value))))) warnings.push('Invalid hours');
  const ids = rows.map((row) => String(row.employeeId ?? row.employee_id ?? row['Employee ID'] ?? '').trim()).filter(Boolean);
  const duplicateIds = ids.filter((id, index) => ids.indexOf(id) !== index);
  if (duplicateIds.length) warnings.push('Duplicate employee row');
  if (rows.some((row) => /unknown|n\/a/i.test(String(row.department ?? row.Department ?? '')))) warnings.push('Unknown department');
  if (headers.some((header) => /^column\d+$/i.test(header))) warnings.push('Unmapped columns');
  return [...new Set(warnings)];
}

export function UploadCenter({
  action,
  datasetType,
  disabledPublish,
  fileName,
  onActionChange,
  onDatasetTypeChange,
  onFile,
  onPublish,
  onStep,
  step
}: UploadCenterProps) {
  const [rows, setRows] = useState<ImportRow[]>([]);
  const [selectedRows, setSelectedRows] = useState<number[]>([]);
  const [filter, setFilter] = useState('');
  const [sortColumn, setSortColumn] = useState('');
  const [loading, setLoading] = useState(false);
  const [progress, setProgress] = useState(0);
  const [message, setMessage] = useState('Upload a CSV or JSON file to generate an import preview. XLS/XLSX files are staged and parsed during backend publish.');
  const [approved, setApproved] = useState(false);
  const headers = useMemo(() => Array.from(new Set(rows.flatMap((row) => Object.keys(row)))).slice(0, 14), [rows]);
  const warnings = useMemo(() => rowWarnings(rows, datasetType), [datasetType, rows]);
  const filteredRows = useMemo(() => {
    const query = filter.toLowerCase().trim();
    const source = query ? rows.filter((row) => Object.values(row).some((value) => value.toLowerCase().includes(query))) : rows;
    if (!sortColumn) return source;
    return [...source].sort((a, b) => String(a[sortColumn] ?? '').localeCompare(String(b[sortColumn] ?? '')));
  }, [filter, rows, sortColumn]);
  const currentStepIndex = Math.max(stepOrder.indexOf(step), 0);
  const maxUnlockedIndex = fileName ? Math.max(currentStepIndex, 1) : 0;

  async function handleFile(file: File | null) {
    onFile(file);
    setRows([]);
    setSelectedRows([]);
    setApproved(false);
    if (!file) {
      setMessage('Import canceled. Choose a file to restart.');
      onStep('upload');
      return;
    }
    setLoading(true);
    setProgress(20);
    try {
      const parsedRows = await parseImportFile(file);
      setRows(parsedRows);
      setProgress(100);
      setMessage(parsedRows.length ? `${parsedRows.length} rows and ${Object.keys(parsedRows[0] ?? {}).length} columns detected.` : `${file.name} staged. Preview will be completed by the server parser during publish.`);
      onStep('preview');
    } catch (error) {
      setMessage(error instanceof Error ? error.message : 'File preview could not be parsed.');
      onStep('preview');
    } finally {
      setLoading(false);
      window.setTimeout(() => setProgress(0), 700);
    }
  }

  function updateCell(rowIndex: number, header: string, value: string) {
    setRows((current) => current.map((row, index) => index === rowIndex ? { ...row, [header]: value } : row));
  }

  function deleteRow(rowIndex: number) {
    setRows((current) => current.filter((_, index) => index !== rowIndex));
    setSelectedRows((current) => current.filter((index) => index !== rowIndex));
  }

  function addRow() {
    setRows((current) => [...current, Object.fromEntries((headers.length ? headers : ['employeeId', 'name', 'department', 'status']).map((header) => [header, '']))]);
  }

  function setStep(nextStep: UploadStep) {
    const nextIndex = stepOrder.indexOf(nextStep);
    if (nextIndex > maxUnlockedIndex + 1 && !approved) return;
    if (nextStep === 'process') {
      setLoading(true);
      setProgress(35);
      setMessage(`${action} processing started for ${datasetType} dataset.`);
      window.setTimeout(() => {
        setProgress(100);
        setLoading(false);
        setMessage(`${rows.length} imported rows processed. ${warnings.length} warning categories reviewed.`);
      }, 500);
    }
    onStep(nextStep);
  }

  function next() {
    const nextStep = stepOrder[Math.min(currentStepIndex + 1, stepOrder.length - 1)];
    setStep(nextStep);
  }

  function previous() {
    onStep(stepOrder[Math.max(currentStepIndex - 1, 0)]);
  }

  function approveImport() {
    setApproved(true);
    setMessage(`Import approved for ${action}. ${rows.length} rows ready for processing.`);
    onStep('process');
  }

  function publish() {
    setMessage('Publishing dataset to the HR workspace...');
    onPublish();
    onStep('publish');
  }

  return (
    <section className="hr-upload-center compact-import-center">
      <div>
        <p className="eyebrow">Dataset Import Center</p>
        <h2>Stage, preview, approve, then publish</h2>
        <span>Imports do not modify HR data until an approver publishes the staged dataset.</span>
      </div>
      <div className="hr-upload-steps">
        {steps.map(([key, label], index) => (
          <button className={`${step === key ? 'active' : ''} ${index < currentStepIndex ? 'completed' : ''}`} disabled={index > maxUnlockedIndex + 1 && !approved} key={key} type="button" onClick={() => setStep(key)}>
            {index < currentStepIndex ? '✓ ' : ''}{label}
          </button>
        ))}
      </div>
      {loading && <div className="progress-track"><span style={{ width: `${progress}%` }} /></div>}
      <div className="attendance-control-bar">
        <label>Dataset Type
          <select value={datasetType} onChange={(event) => onDatasetTypeChange(event.target.value)}>
            {datasetTypes.map((type) => <option key={type}>{type}</option>)}
          </select>
        </label>
        <label>Action
          <select value={action} onChange={(event) => onActionChange(event.target.value)}>
            {uploadActions.map((option) => <option key={option}>{option}</option>)}
          </select>
        </label>
        <label>Upload File
          <input accept=".csv,.xlsx,.xls,.json,.pdf" type="file" onChange={(event) => void handleFile(event.target.files?.[0] ?? null)} />
        </label>
        <button type="button" disabled={!fileName} onClick={previous}>Previous</button>
        <button type="button" disabled={!fileName} onClick={next}>Next</button>
        <button className="ghost-button compact" type="button" disabled={!fileName} onClick={() => setMessage('Import draft saved in the browser workflow. Publish to persist it to HR datasets.')}>Save Draft</button>
      </div>
      <div className="dataset-merge-preview import-status-panel">
        <strong>{fileName ? `Preview: ${fileName}` : 'No file staged'}</strong>
        <span>{message}</span>
        <div className="dataset-detail-grid compact">
          <div><span>Rows</span><strong>{rows.length}</strong></div>
          <div><span>Columns</span><strong>{headers.length}</strong></div>
          <div><span>Warnings</span><strong>{warnings.length}</strong></div>
          <div><span>Action</span><strong>{action}</strong></div>
        </div>
        <div className="workflow-history-strip">
          {warnings.map((warning) => <span key={warning}>Warning: {warning}</span>)}
          {!warnings.length && <span>No blocking validation warnings detected.</span>}
        </div>
        {['preview', 'validate', 'edit', 'approve', 'process', 'publish'].includes(step) && (
          <>
            <div className="record-toolbar preview-toolbar">
              <input placeholder="Filter preview rows" value={filter} onChange={(event) => setFilter(event.target.value)} />
              <select value={sortColumn} onChange={(event) => setSortColumn(event.target.value)}>
                <option value="">No sorting</option>
                {headers.map((header) => <option key={header} value={header}>{header}</option>)}
              </select>
              <button type="button" onClick={addRow}>Add Row</button>
              <button type="button" disabled={!selectedRows.length} onClick={() => setMessage(`${selectedRows.length} selected rows approved.`)}>Approve Rows</button>
              <button type="button" disabled={!selectedRows.length} onClick={() => setRows((current) => current.filter((_, index) => !selectedRows.includes(index)))}>Reject/Delete Selected</button>
            </div>
            <div className="hr-grid-scroll import-preview-grid">
              <table>
                <thead>
                  <tr>
                    <th>Select</th>
                    {headers.map((header) => <th key={header}><button type="button" onClick={() => setSortColumn(header)}>{header}</button></th>)}
                    <th>Row Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {filteredRows.slice(0, 25).map((row, rowIndex) => {
                    const sourceIndex = rows.indexOf(row);
                    return (
                      <tr className={selectedRows.includes(sourceIndex) ? 'selected-row' : ''} key={`${rowIndex}-${sourceIndex}`}>
                        <td><input checked={selectedRows.includes(sourceIndex)} type="checkbox" onChange={() => setSelectedRows((current) => current.includes(sourceIndex) ? current.filter((index) => index !== sourceIndex) : [...current, sourceIndex])} /></td>
                        {headers.map((header) => (
                          <td key={header}>
                            <EditableCell disabled={!['edit', 'approve'].includes(step)} value={String(row[header] ?? '')} onCommit={(value) => updateCell(sourceIndex, header, value)} />
                          </td>
                        ))}
                        <td><button className="ghost-button compact danger" type="button" onClick={() => deleteRow(sourceIndex)}>Delete</button></td>
                      </tr>
                    );
                  })}
                  {!filteredRows.length && <tr><td colSpan={headers.length + 2}>No preview rows available. CSV and JSON preview immediately; XLS/XLSX files publish through backend parsing.</td></tr>}
                </tbody>
              </table>
            </div>
          </>
        )}
        <div className="inline-actions">
          <button className="ghost-button compact" type="button" onClick={() => setStep('edit')} disabled={!fileName}>Edit Before Import</button>
          <button className="ghost-button compact" type="button" onClick={approveImport} disabled={!fileName}>Approve Import</button>
          <button type="button" disabled={disabledPublish || !approved} onClick={publish}>Publish Dataset</button>
          <button className="ghost-button compact danger" type="button" onClick={() => void handleFile(null)}>Cancel</button>
        </div>
      </div>
    </section>
  );
}
