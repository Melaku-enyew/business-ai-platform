const emptyValues = new Set(['', 'null', 'n/a', 'na', 'undefined']);

export const cleanupStatuses = ['pending', 'processing', 'completed', 'failed'];

export function cleanDataset(dataset) {
  const originalHeaders = Array.isArray(dataset.headers) ? dataset.headers : [];
  const columnMap = buildColumnMap(originalHeaders);
  const standardizedHeaders = Object.values(columnMap);
  const seenRows = new Set();
  const logs = [];
  const cleanedRecords = [];
  const metrics = {
    duplicatesRemoved: 0,
    rowsFixed: 0,
    invalidValuesDetected: 0,
    columnsStandardized: originalHeaders.filter((header) => columnMap[header] !== header).length,
    totalCleanedRows: 0
  };

  for (const record of dataset.records ?? []) {
    const normalized = {};
    let rowChanged = false;
    let nonEmptyCount = 0;

    for (const header of originalHeaders) {
      const targetHeader = columnMap[header];
      const rawValue = record?.[header];
      const normalizedValue = normalizeValue(rawValue, header);

      normalized[targetHeader] = normalizedValue.value;
      if (!normalizedValue.isEmpty) nonEmptyCount += 1;
      if (normalizedValue.changed || targetHeader !== header) rowChanged = true;
      if (normalizedValue.invalid) metrics.invalidValuesDetected += 1;
    }

    if (nonEmptyCount === 0) {
      rowChanged = true;
      metrics.rowsFixed += 1;
      logs.push('Removed an empty row.');
      continue;
    }

    const rowKey = JSON.stringify(normalized);
    if (seenRows.has(rowKey)) {
      metrics.duplicatesRemoved += 1;
      logs.push('Removed a duplicate row.');
      continue;
    }
    seenRows.add(rowKey);

    if (rowChanged) metrics.rowsFixed += 1;
    cleanedRecords.push(normalized);
  }

  metrics.totalCleanedRows = cleanedRecords.length;
  if (metrics.columnsStandardized > 0) {
    logs.push(`Standardized ${metrics.columnsStandardized} column names.`);
  }
  if (metrics.invalidValuesDetected > 0) {
    logs.push(`Detected ${metrics.invalidValuesDetected} invalid numeric values.`);
  }
  logs.push(`Cleaned ${metrics.totalCleanedRows} rows for AI-ready analytics.`);

  return {
    headers: standardizedHeaders,
    records: cleanedRecords,
    metrics,
    logs: [...new Set(logs)],
    preview: {
      before: (dataset.preview ?? dataset.records ?? []).slice(0, 5),
      after: cleanedRecords.slice(0, 5)
    },
    operations: [
      'duplicate_removal',
      'null_empty_row_handling',
      'whitespace_trimming',
      'date_normalization',
      'numeric_validation',
      'column_standardization',
      'basic_value_normalization'
    ],
    // TODO: Add AI recommendations once LLM-backed profiling is enabled.
    // TODO: Add anomaly detection over cleaned numeric and date columns.
    // TODO: Add predictive analytics using cleaned historical records.
    // TODO: Add automated report summaries for cleaned dataset exports.
    futureAiReady: true
  };
}

export function recordsToCsv(headers, records) {
  const safeHeaders = Array.isArray(headers) ? headers : [];
  const lines = [
    safeHeaders.map(escapeCsvValue).join(','),
    ...(records ?? []).map((record) => safeHeaders.map((header) => escapeCsvValue(record?.[header] ?? '')).join(','))
  ];
  return `${lines.join('\n')}\n`;
}

function buildColumnMap(headers) {
  const seen = new Map();
  return headers.reduce((map, header, index) => {
    const fallback = `column_${index + 1}`;
    const base = String(header || fallback)
      .trim()
      .replace(/\s+/g, '_')
      .replace(/[^a-zA-Z0-9_]/g, '')
      .replace(/_{2,}/g, '_')
      .replace(/^_+|_+$/g, '')
      .toLowerCase() || fallback;
    const count = seen.get(base) ?? 0;
    seen.set(base, count + 1);
    map[header] = count ? `${base}_${count + 1}` : base;
    return map;
  }, {});
}

function normalizeValue(value, header) {
  const raw = value == null ? '' : String(value);
  const trimmed = raw.trim();
  const compact = trimmed.replace(/\s+/g, ' ');
  const lower = compact.toLowerCase();
  const isEmpty = emptyValues.has(lower);

  if (isEmpty) {
    return { value: '', changed: raw !== '', invalid: false, isEmpty: true };
  }

  const normalizedDate = normalizeDate(compact);
  if (normalizedDate) {
    return { value: normalizedDate, changed: normalizedDate !== raw, invalid: false, isEmpty: false };
  }

  const likelyNumeric = isLikelyNumericColumn(header) || /^[($-]?\d[\d,]*(\.\d+)?%?\)?$/.test(compact);
  if (likelyNumeric) {
    const numeric = Number(compact.replace(/[$,%\s]/g, '').replace(/^\((.*)\)$/, '-$1'));
    if (Number.isFinite(numeric)) {
      const normalizedNumber = String(numeric);
      return { value: normalizedNumber, changed: normalizedNumber !== raw, invalid: false, isEmpty: false };
    }
    return { value: compact, changed: compact !== raw, invalid: true, isEmpty: false };
  }

  const normalizedBoolean = normalizeBoolean(compact);
  if (normalizedBoolean) {
    return { value: normalizedBoolean, changed: normalizedBoolean !== raw, invalid: false, isEmpty: false };
  }

  return { value: compact, changed: compact !== raw, invalid: false, isEmpty: false };
}

function normalizeDate(value) {
  if (!/date|^\d{1,4}[/-]\d{1,2}[/-]\d{1,4}$/i.test(value)) {
    return '';
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return '';
  }
  return parsed.toISOString().slice(0, 10);
}

function normalizeBoolean(value) {
  const lower = value.toLowerCase();
  if (['yes', 'y', 'true'].includes(lower)) return 'true';
  if (['no', 'n', 'false'].includes(lower)) return 'false';
  return '';
}

function isLikelyNumericColumn(header) {
  return /(amount|total|price|cost|revenue|sales|qty|quantity|count|number|rate|percent|score|hours|balance)/i.test(String(header));
}

function escapeCsvValue(value) {
  const text = String(value ?? '');
  return /[",\n\r]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
}
