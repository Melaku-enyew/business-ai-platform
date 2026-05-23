type UploadStep = 'upload' | 'preview' | 'action' | 'validate' | 'approve' | 'process' | 'publish';

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
  ['approve', '5 Approve'],
  ['process', '6 Run Processing'],
  ['publish', '7 Export / Publish']
];

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
  return (
    <section className="hr-upload-center compact-import-center">
      <div>
        <p className="eyebrow">Dataset Import Center</p>
        <h2>Stage, preview, approve, then publish</h2>
        <span>Imports do not modify HR data until an approver publishes the staged dataset.</span>
      </div>
      <div className="hr-upload-steps">
        {steps.map(([key, label]) => (
          <button className={step === key ? 'active' : ''} key={key} type="button" onClick={() => onStep(key)}>
            {label}
          </button>
        ))}
      </div>
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
          <input accept=".csv,.xlsx,.xls,.json,.pdf" type="file" onChange={(event) => onFile(event.target.files?.[0] ?? null)} />
        </label>
        <button type="button" disabled={disabledPublish} onClick={onPublish}>Approve Import</button>
      </div>
      <div className="dataset-merge-preview">
        <strong>{fileName ? `Preview: ${fileName}` : 'No file staged'}</strong>
        <span>Matched rows, missing fields, duplicate employees, invalid columns, unmapped columns, and overwrite warnings appear here before approval.</span>
        <div className="inline-actions">
          <button className="ghost-button compact" type="button" onClick={() => onStep('approve')} disabled={!fileName}>Edit Before Import</button>
          <button className="ghost-button compact danger" type="button" onClick={() => onFile(null)}>Cancel</button>
        </div>
      </div>
    </section>
  );
}
