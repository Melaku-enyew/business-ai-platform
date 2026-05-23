export type DatasetStoreSnapshot = {
  activeDatasetId: string;
  selectedRowIds: string[];
};

export const datasetStoreDefaults: DatasetStoreSnapshot = {
  activeDatasetId: '',
  selectedRowIds: []
};
