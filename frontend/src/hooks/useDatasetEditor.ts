import { useState } from 'react';

export function useDatasetEditor<T extends Record<string, string>>(initialRows: T[] = []) {
  const [rows, setRows] = useState<T[]>(initialRows);
  const [dirty, setDirty] = useState(false);

  function replaceRows(nextRows: T[]) {
    setRows(nextRows);
    setDirty(true);
  }

  function resetRows(nextRows: T[]) {
    setRows(nextRows);
    setDirty(false);
  }

  return { dirty, replaceRows, resetRows, rows, setDirty };
}
