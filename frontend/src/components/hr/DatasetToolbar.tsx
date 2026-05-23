type DatasetToolbarProps = {
  canManage: boolean;
  onAddEmployee: () => void;
  onApprove: () => void;
  onBulk: () => void;
  onEdit: () => void;
  onExport: () => void;
  onReports: () => void;
  onUpload: () => void;
};

export function DatasetToolbar({
  canManage,
  onAddEmployee,
  onApprove,
  onBulk,
  onEdit,
  onExport,
  onReports,
  onUpload
}: DatasetToolbarProps) {
  return (
    <section className="hr-modern-toolbar" aria-label="HR dataset actions">
      <div className="toolbar-group primary">
        <span>Primary</span>
        <button type="button" onClick={onAddEmployee} disabled={!canManage}>+ Add</button>
        <button type="button" onClick={onUpload}>Upload</button>
        <button type="button" onClick={onEdit}>Edit</button>
      </div>
      <div className="toolbar-group secondary">
        <span>Secondary</span>
        <button type="button" onClick={onReports}>Reports</button>
        <button type="button" onClick={onExport}>Export</button>
      </div>
      <div className="toolbar-group overflow">
        <span>Workflow</span>
        <button type="button" onClick={onApprove} disabled={!canManage}>Approve</button>
        <button type="button" onClick={onBulk} disabled={!canManage}>More actions</button>
      </div>
    </section>
  );
}
