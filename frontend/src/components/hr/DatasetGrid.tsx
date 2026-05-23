export type HrDatasetSummary = {
  id: string;
  fileName: string;
  type: string;
  rows: number;
  status: string;
  updatedAt: string;
};

type DatasetGridProps = {
  activeDatasetId: string;
  datasets: HrDatasetSummary[];
  onAction: (datasetId: string, action: string) => void;
  onOpen: (datasetId: string) => void;
};

export function DatasetGrid({ activeDatasetId, datasets, onAction, onOpen }: DatasetGridProps) {
  return (
    <section className="hr-compact-dataset-grid">
      <div className="hr-dataset-grid-head">
        <span>Dataset</span>
        <span>Type</span>
        <span>Rows</span>
        <span>Status</span>
        <span>Updated</span>
        <span>Actions</span>
      </div>
      {datasets.map((dataset) => (
        <article className={dataset.id === activeDatasetId ? 'active' : ''} key={dataset.id}>
          <button type="button" onClick={() => onOpen(dataset.id)}>
            <strong>{dataset.fileName}</strong>
          </button>
          <span>{dataset.type}</span>
          <span>{dataset.rows.toLocaleString()}</span>
          <span className={`status-pill ${dataset.status.replace(/\s+/g, '-')}`}>{dataset.status}</span>
          <span>{new Date(dataset.updatedAt).toLocaleString()}</span>
          <div className="inline-actions">
            {['Open', 'Edit', 'Approve', 'Export', 'Archive'].map((action) => (
              <button className="ghost-button compact" key={action} type="button" onClick={() => onAction(dataset.id, action.toLowerCase())}>
                {action}
              </button>
            ))}
          </div>
        </article>
      ))}
      {!datasets.length && <div className="hr-empty-line">No HR datasets yet. Upload or create a dataset to begin.</div>}
    </section>
  );
}
