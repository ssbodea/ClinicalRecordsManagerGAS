const CONFIG = {
  SHEET_NAME: "Form Responses 1",
  ID_COLUMN: "ID",
  VERIFY_RANGE: 5,
  START_ID: 1,
  ID_INCREMENT: 1,
  MAX_RETRIES: 3,
  BATCH_SIZE: 500,
  CACHE_EXPIRATION: 21600,
  CACHE_PREFIX: "ID_",
  PROPS_PREFIX: "ID_",
  TIMESTAMP_SEARCH_BATCH: 100
};

function getValue(row, colIndex, defaultValue = '') {
  if (colIndex < 0) return defaultValue;
  const val = row[colIndex];
  return val !== null && val !== undefined ? val.toString().trim() : defaultValue;
}

function formatBtCas(code, specialty, type) {
  return code ? `${code} | ${specialty} | ${type}` : '';
}

function parseBtCas(fieldValue) {
  if (!fieldValue) return ['', '', ''];
  const parts = fieldValue.includes('|')
    ? fieldValue.split('|').map(part => part.trim())
    : [fieldValue];
  return [...parts, '', ''].slice(0, 3);
}

function parseDateFromSheet(dateString, includeTime = false) {
  if (!dateString) return '';
  const date = new Date(dateString);
  if (isNaN(date.getTime())) return dateString;

  const pad = n => n.toString().padStart(2, '0');
  const day = pad(date.getDate());
  const month = pad(date.getMonth() + 1);
  const year = date.getFullYear();

  if (includeTime) {
    const hours = pad(date.getHours());
    const minutes = pad(date.getMinutes());
    const seconds = pad(date.getSeconds());
    return `${day}/${month}/${year} ${hours}:${minutes}:${seconds}`;
  }
  return `${day}/${month}/${year}`;
}

function formatDateToDDMMYYYY(dateStr) {
  if (!dateStr) return '';
  const date = new Date(dateStr);
  const day = String(date.getDate()).padStart(2, '0');
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const year = date.getFullYear();
  return `${day}/${month}/${year}`;
}

function formatDateToYYYYMMDD(dateStr) {
  if (!dateStr) return '';
  const [day, month, year] = dateStr.split('/');
  return `${year}-${month}-${day}`;
}

function parseDateRange(rangeStr, part = 'start') {
  if (!rangeStr || !rangeStr.includes('-')) return '';
  const [start, end] = rangeStr.split('-').map(s => s.trim());
  const dateStr = part === 'start' ? start : end;
  return formatDateToYYYYMMDD(dateStr);
}

function createDateRange(startDate, endDate) {
  if (!startDate && !endDate) return '';
  return `${formatDateToDDMMYYYY(startDate)} - ${formatDateToDDMMYYYY(endDate)}`;
}
class IDManager {
  constructor() {
    this.sheet = SpreadsheetApp.getActive().getSheetByName(CONFIG.SHEET_NAME);
    this.headers = this.sheet.getRange(1, 1, 1, this.sheet.getLastColumn()).getValues()[0];
    this.idColumn = this.headers.indexOf(CONFIG.ID_COLUMN) + 1;
    if (this.idColumn === 0) throw new Error(`ID column "${CONFIG.ID_COLUMN}" not found`);
    this.lock = LockService.getScriptLock();
    this.documentLock = LockService.getDocumentLock();
    this.cache = CacheService.getScriptCache();
    this.props = PropertiesService.getScriptProperties();
  }

  onFormSubmit(e) {
    if (!this._isValidSubmission(e)) return null;

    for (let retry = 0; retry < CONFIG.MAX_RETRIES; retry++) {
      try {
        this._acquireLocks();
        const lastId = this._getLastId();
        const newId = lastId + CONFIG.ID_INCREMENT;

        this._updatePatientId(e.range.getRow(), newId);
        return newId;
      } catch (error) {
        if (retry >= CONFIG.MAX_RETRIES - 1) {
          // No row deletion on failure - just return null
          return null;
        }
        Utilities.sleep(2000 * (retry + 1));
      } finally {
        this._releaseLocks();
      }
    }
    return null;
  }

  nightlyFixAll() {
    try {
      this.documentLock.waitLock(30000);
      const lastRow = this.sheet.getLastRow();
      let expectedId = CONFIG.START_ID;

      for (let row = 2; row <= lastRow; row += CONFIG.BATCH_SIZE) {
        const batchEnd = Math.min(row + CONFIG.BATCH_SIZE - 1, lastRow);
        const range = this.sheet.getRange(row, this.idColumn, batchEnd - row + 1, 1);
        const values = this._generateFixedIds(range, expectedId);
        range.setValues(values);
        expectedId = values[values.length - 1][0] + CONFIG.ID_INCREMENT;
        Utilities.sleep(1000);
      }

      if (lastRow >= 2) {
        const lastId = this._strictParseInt(this.sheet.getRange(lastRow, this.idColumn).getValue());
        if (!isNaN(lastId)) this._updateIdStorage(lastId);
      }
    } finally {
      this.documentLock.releaseLock();
    }
  }

  _isValidSubmission(e) {
    return e?.range && e.range.getRow() >= 2;
  }

  _acquireLocks() {
    this.documentLock.waitLock(10000);
    this.lock.waitLock(10000);
  }

  _releaseLocks() {
    this.lock.releaseLock();
    this.documentLock.releaseLock();
  }

  _getLastId() {
    let lastId = this._getLastIdFromCache();
    if (isNaN(lastId)) lastId = this._getLastIdFromProperties();
    if (isNaN(lastId)) {
      const lastRow = this.sheet.getLastRow();
      lastId = lastRow < 2 ? CONFIG.START_ID - CONFIG.ID_INCREMENT : this._strictParseInt(this.sheet.getRange(lastRow, this.idColumn).getValue());
    }
    return isNaN(lastId) ? CONFIG.START_ID - CONFIG.ID_INCREMENT : lastId;
  }

  _updatePatientId(row, newId) {
    this.sheet.getRange(row, this.idColumn).setValue(newId);
    this._updateIdStorage(newId);
  }

  _generateFixedIds(range, expectedId) {
    return range.getValues().map(([id]) => {
      const currentId = this._strictParseInt(id);
      const fixedId = isNaN(currentId) || currentId !== expectedId ? expectedId : currentId;
      expectedId = fixedId + CONFIG.ID_INCREMENT;
      return [fixedId];
    });
  }

  _getLastIdFromCache() {
    const cachedId = this.cache.get(CONFIG.CACHE_PREFIX + "LAST_ID");
    return cachedId ? this._strictParseInt(cachedId) : NaN;
  }

  _getLastIdFromProperties() {
    const propId = this.props.getProperty(CONFIG.PROPS_PREFIX + "LAST_ID");
    return propId ? this._strictParseInt(propId) : NaN;
  }

  _updateIdStorage(newId) {
    this.cache.put(CONFIG.CACHE_PREFIX + "LAST_ID", newId.toString(), CONFIG.CACHE_EXPIRATION);
    this.props.setProperty(CONFIG.PROPS_PREFIX + "LAST_ID", newId.toString());
  }

  _strictParseInt(value) {
    if (value === null || value === undefined) return NaN;
    const num = Number(value);
    return Number.isSafeInteger(num) ? num : NaN;
  }

  _resetStorage() {
    try {
      this._acquireLocks();
      this.cache.removeAll([CONFIG.CACHE_PREFIX + "LAST_ID"]);
      this.props.deleteProperty(CONFIG.PROPS_PREFIX + "LAST_ID");
      return true;
    } catch (error) {
      console.error("Error resetting storage:", error);
      return false;
    } finally {
      this._releaseLocks();
    }
  }
}

function onFormSubmit(e) {
  return new IDManager().onFormSubmit(e);
}

function nightlyFixAll() {
  new IDManager().nightlyFixAll();
}

function resetIDStorage() {
  try {
    const manager = new IDManager();
    const success = manager._resetStorage();
    if (success) {
      console.log("Successfully reset ID storage (cache and properties)");
      return "ID storage reset successfully";
    } else {
      console.warn("Failed to reset ID storage");
      return "Failed to reset ID storage";
    }
  } catch (error) {
    console.error("Error in resetIDStorage:", error);
    return "Error resetting ID storage: " + error.message;
  }
}

function doGet() {
  try {
    return HtmlService.createHtmlOutputFromFile('Index').setTitle('UMF Registru Medical');
  } catch (e) {
    return ContentService.createTextOutput(e.message).setMimeType(ContentService.MimeType.TEXT);
  }
}

function deletePatientData(password) {
  const correctPassword = PropertiesService.getScriptProperties().getProperty('DELETE_PASSWORD');
  if (!correctPassword) throw new Error("Parola nu este setată");
  if (password !== correctPassword) throw new Error("Parolă incorectă");

  const sheet = SpreadsheetApp.getActive().getSheetByName(CONFIG.SHEET_NAME);
  if (!sheet) throw new Error("Foaia nu există");

  const lastRow = sheet.getLastRow();
  if (lastRow <= 1) return;
  sheet.deleteRows(2, lastRow - 1);
}
function getColumnMappings(headers) {
  if (!Array.isArray(headers)) {
    throw new Error('Headers must be an array');
  }

  const mappings = {};

  const headerDefinitions = [
    { key: 'id', displayName: 'ID' },
    { key: 'timestamp', displayName: 'Timestamp' },
    { key: 'fullName', displayName: 'Full Name' },
    { key: 'age', displayName: 'Age' },
    { key: 'gender', displayName: 'Gender' },
    { key: 'lmp', displayName: 'LMP' },
    { key: 'email', displayName: 'Email' },
    { key: 'phone', displayName: 'Phone' },
    { key: 'address', displayName: 'Address' },
    { key: 'faculty', displayName: 'Faculty' },
    { key: 'year', displayName: 'Year' },
    { key: 'language', displayName: 'Language' },
    { key: 'symptoms', displayName: 'Symptoms / Visit Reason' },
    { key: 'treatment', displayName: 'Current Treatment' },
    { key: 'chronic', displayName: 'Chronic Diseases' },
    { key: 'allergies', displayName: 'Allergies' },
    { key: 'diagnosis', displayName: 'diagnosis' },
    { key: 'codes1', displayName: 'codes1' },
    { key: 'rpIntegrala', displayName: 'rpIntegrala' },
    { key: 'rpGratuita', displayName: 'rpGratuita' },
    { key: 'btCas1', displayName: 'btCas1' },
    { key: 'btCas2', displayName: 'btCas2' },
    { key: 'btCas3', displayName: 'btCas3' },
    { key: 'btSimplu', displayName: 'btSimplu' },
    { key: 'amAbsenta', displayName: 'amAbsenta' },
    { key: 'amSport', displayName: 'amSport' },
    { key: 'amAlt', displayName: 'amAlt' },
    { key: 'amBursa', displayName: 'amBursa' },
    { key: 'aeAviz', displayName: 'aeAviz' },
    { key: 'ebInaltime', displayName: 'ebInaltime' },
    { key: 'ebGreutate', displayName: 'ebGreutate' },
    { key: 'ebIMC', displayName: 'ebIMC' },
    { key: 'codes2', displayName: 'codes2' }
  ];

  headerDefinitions.forEach(({ key, displayName }) => {
    const index = headers.indexOf(displayName);
    if (index === -1) {
      console.warn(`Column "${displayName}" not found in headers`);
    }
    mappings[key] = index;
  });

  return mappings;
}

function createPatientObject(row, cols) {

  const patient = {
    id: getValue(row, cols.id),
    timestamp: parseDateFromSheet(getValue(row, cols.timestamp), true),
    fullName: getValue(row, cols.fullName),
    age: getValue(row, cols.age),
    gender: getValue(row, cols.gender),
    lmp: parseDateFromSheet(getValue(row, cols.lmp), false),
    email: getValue(row, cols.email),
    phone: getValue(row, cols.phone),
    address: getValue(row, cols.address),
    faculty: getValue(row, cols.faculty),
    year: getValue(row, cols.year),
    language: getValue(row, cols.language),
    symptoms: getValue(row, cols.symptoms),
    treatment: getValue(row, cols.treatment),
    chronic: getValue(row, cols.chronic),
    allergies: getValue(row, cols.allergies),
    diagnosis: getValue(row, cols.diagnosis),
    codes1: getValue(row, cols.codes1),
    rpIntegrala: getValue(row, cols.rpIntegrala),
    rpGratuita: getValue(row, cols.rpGratuita),
    btCas1: getValue(row, cols.btCas1),
    specialitate1: '',
    tip1: '',
    btCas2: getValue(row, cols.btCas2),
    specialitate2: '',
    tip2: '',
    btCas3: getValue(row, cols.btCas3),
    specialitate3: '',
    tip3: '',
    btSimplu: getValue(row, cols.btSimplu),
    amAbsentaStart: parseDateRange(getValue(row, cols.amAbsenta), 'start'),
    amAbsentaEnd: parseDateRange(getValue(row, cols.amAbsenta), 'end'),
    amSportStart: parseDateRange(getValue(row, cols.amSport), 'start'),
    amSportEnd: parseDateRange(getValue(row, cols.amSport), 'end'),
    amAlt: getValue(row, cols.amAlt),
    amBursa: String(getValue(row, cols.amBursa)).toUpperCase() === 'Y',
    aeAviz: String(getValue(row, cols.aeAviz)).toUpperCase() === 'Y',
    ebInaltime: getValue(row, cols.ebInaltime),
    ebGreutate: getValue(row, cols.ebGreutate),
    ebIMC: getValue(row, cols.ebIMC),
    codes2: getValue(row, cols.codes2)
  };

  [patient.btCas1, patient.specialitate1, patient.tip1] = parseBtCas(patient.btCas1);
  [patient.btCas2, patient.specialitate2, patient.tip2] = parseBtCas(patient.btCas2);
  [patient.btCas3, patient.specialitate3, patient.tip3] = parseBtCas(patient.btCas3);

  return patient;
}
async function loadTodaysPatients() {
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(CONFIG.SHEET_NAME);
  if (!sheet || sheet.getLastRow() <= 1) return [];

  const headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
  const cols = getColumnMappings(headers);
  if (cols.timestamp === -1) return [];

  // Use script timezone for today's date
  const todayStart = new Date();
  todayStart.setHours(0, 0, 0, 0);

  const todaysPatients = [];
  const lastRow = sheet.getLastRow();

  // Start from the last row and go backwards
  for (let rowNum = lastRow; rowNum >= 2; rowNum--) {
    const row = sheet.getRange(rowNum, 1, 1, headers.length).getValues()[0];
    const timestampStr = getValue(row, cols.timestamp);
    const timestamp = timestampStr ? new Date(timestampStr) : null;

    if (!timestamp) continue; // Skip invalid timestamps

    // Stop and return reversed array when we encounter a date before today
    if (timestamp < todayStart) {
      return todaysPatients.reverse();
    }

    // If we're here, it's today's date
    const patient = createPatientObject(row, cols);
    todaysPatients.push(patient);
  }

  // If we reach the beginning without finding older dates, return reversed
  return todaysPatients.reverse();
}
function savePatientData(patientData) {
  const spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = spreadsheet.getSheetByName(CONFIG.SHEET_NAME);

  const idRange = sheet.getRange(2, 1, sheet.getLastRow() - 1, 1);
  const ids = idRange.getValues().flat();
  const rowIndex = binarySearch(ids, patientData.id);

  if (rowIndex === -1) {
    throw new Error(`Pacientul cu ID ${patientData.id} nu a fost găsit în baza de date.`);
  }

  const sheetRow = rowIndex + 2;

  sheet.getRange(sheetRow, 17, 1, 17).setValues([[
    patientData.diagnosis || '',
    patientData.codes1 || '',
    patientData.rpIntegrala || '',
    patientData.rpGratuita || '',
    formatBtCas(patientData.btCas1, patientData.specialitate1, patientData.tip1),
    formatBtCas(patientData.btCas2, patientData.specialitate2, patientData.tip2),
    formatBtCas(patientData.btCas3, patientData.specialitate3, patientData.tip3),
    patientData.btSimplu || '',
    createDateRange(patientData.amAbsentaStart, patientData.amAbsentaEnd),
    createDateRange(patientData.amSportStart, patientData.amSportEnd),
    patientData.amAlt || '',
    patientData.amBursa ? 'Y' : '',
    patientData.aeAviz ? 'Y' : '',
    patientData.ebInaltime || '',
    patientData.ebGreutate || '',
    patientData.ebIMC || '',
    patientData.codes2 || ''
  ]]);
}

function binarySearch(ids, targetId) {
  let left = 0;
  let right = ids.length - 1;

  while (left <= right) {
    const mid = Math.floor((left + right) / 2);
    const midId = ids[mid];

    if (midId == targetId) {
      return mid;
    } else if (midId < targetId) {
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }

  return -1;
}
function searchPatients(searchName, searchEmail, searchPhone) {
  const sheet = SpreadsheetApp.getActive().getSheetByName(CONFIG.SHEET_NAME);
  if (!sheet || sheet.getLastRow() <= 1) return [];

  const headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
  const cols = getColumnMappings(headers);
  const data = sheet.getRange(2, 1, sheet.getLastRow() - 1, headers.length).getValues();

  const hasName = searchName && cols.fullName >= 0;
  const hasEmail = searchEmail && cols.email >= 0;
  const hasPhone = searchPhone && cols.phone >= 0;

  const nameTerms = hasName ? searchName.toString().trim().toLowerCase().split(' ') : [];
  const emailTerm = hasEmail ? searchEmail.toString().trim().toLowerCase() : null;
  const phoneTerm = hasPhone ? searchPhone.toString().trim() : null;

  return data.filter(row => {
    if (hasEmail) {
      const email = row[cols.email]?.toString().trim().toLowerCase() || '';
      if (email.indexOf(emailTerm) === -1) return false;
    }

    if (hasPhone) {
      const phone = row[cols.phone]?.toString().trim() || '';
      if (phone.indexOf(phoneTerm) === -1) return false;
    }

    if (hasName) {
      const fullName = row[cols.fullName]?.toString().trim().toLowerCase() || '';
      for (const term of nameTerms) {
        if (fullName.indexOf(term) === -1) return false;
      }
    }

    return true;
  })
    .map(row => createPatientObject(row, cols))
    .reverse();
}
function findStartRow(data, timestampCol, startDate) {
  let low = 1;
  let high = data.length - 1;
  let result = data.length;

  while (low <= high) {
    const mid = Math.floor((low + high) / 2);
    const rowDate = new Date(data[mid][timestampCol]);

    if (rowDate < startDate) {
      low = mid + 1;
    } else {
      result = mid;
      high = mid - 1;
    }
  }
  return result;
}

function findEndRow(data, timestampCol, endDate) {
  let low = 1;
  let high = data.length - 1;
  let result = 0;

  while (low <= high) {
    const mid = Math.floor((low + high) / 2);
    const rowDate = new Date(data[mid][timestampCol]);

    if (rowDate <= endDate) {
      result = mid;
      low = mid + 1;
    } else {
      high = mid - 1;
    }
  }
  return result;
}

function exportPatientData(startDate, endDate) {
  try {
    const exportName = `Export_${parseDateFromSheet(startDate, true)}_${parseDateFromSheet(endDate, true)}`;
    const ss = SpreadsheetApp.getActiveSpreadsheet();

    const existingSheet = ss.getSheetByName(exportName);
    if (existingSheet) ss.deleteSheet(existingSheet);
    const sheet = ss.insertSheet(exportName);
    const CM_TO_PIXELS = 37.8;

    const sourceSheet = ss.getSheetByName(CONFIG.SHEET_NAME);
    const allData = sourceSheet.getDataRange().getValues();
    const allHeaders = allData[0];
    const cols = getColumnMappings(allHeaders);

    const startDateTime = new Date(startDate);
    const endDateTime = new Date(endDate);
    const startRow = findStartRow(allData, cols.timestamp, startDateTime);
    const endRow = findEndRow(allData, cols.timestamp, endDateTime);

    if (startRow > endRow || endRow < 1) {
      sheet.appendRow(["No data found for the selected date range."]);
      return exportName;
    }

    const filteredData = allData.slice(startRow, endRow + 1);

    const exportConfig = [
      { header: 'Nr. crt.', width: 1.5 * CM_TO_PIXELS, colKey: 'id' },
      {
        header: 'Ziua',
        width: 3 * CM_TO_PIXELS,
        transform: (row) => row[cols.timestamp] ? parseDateFromSheet(row[cols.timestamp], false) : ''
      },
      { header: 'Numele și prenumele', width: 4 * CM_TO_PIXELS, colKey: 'fullName' },
      { header: 'Vârsta', width: 1.5 * CM_TO_PIXELS, colKey: 'age' },
      { header: 'Sexul', width: 1.5 * CM_TO_PIXELS, colKey: 'gender' },
      { header: 'Domiciliul', width: 8 * CM_TO_PIXELS, colKey: 'address' },
      {
        header: 'Ocupație',
        width: 3 * CM_TO_PIXELS,
        transform: (row) => row[cols.faculty] ? `${row[cols.faculty]} ${row[cols.year]} ${row[cols.language]}` : ''
      },
      { header: 'Simptome', width: 5.5 * CM_TO_PIXELS, colKey: 'symptoms' },
      { header: 'Diagnostic', width: 12 * CM_TO_PIXELS, colKey: 'diagnosis' },
      { header: 'Cod', width: 4 * CM_TO_PIXELS, colKey: 'codes1' },
      {
        header: 'Prescripții',
        width: 12 * CM_TO_PIXELS,
        transform: (row) => [
          row[cols.rpIntegrala] && `RP Integrală: ${row[cols.rpIntegrala]}`,
          row[cols.rpGratuita] && `RP Gratuită: ${row[cols.rpGratuita]}`,
          row[cols.btCas1] && `BT CAS1: ${row[cols.btCas1]}`,
          row[cols.btCas2] && `BT CAS2: ${row[cols.btCas2]}`,
          row[cols.btCas3] && `BT CAS3: ${row[cols.btCas3]}`,
          row[cols.btSimplu] && `BT Simplu: ${row[cols.btSimplu]}`,
          row[cols.amAbsenta] && `AM Absență: ${row[cols.amAbsenta]}`,
          row[cols.amSport] && `AM Sport: ${row[cols.amSport]}`,
          row[cols.amAlt] && `AM Alt: ${row[cols.amAlt]}`,
          row[cols.amBursa] && 'AM Bursă',
          row[cols.aeAviz] && 'AE Aviz',
          row[cols.ebInaltime] && `EB: ${row[cols.ebInaltime]} cm | ${row[cols.ebGreutate]} kg | ${row[cols.ebIMC]} imc | Cod: ${row[cols.codes2]}`
        ]
          .filter(Boolean)
          .join('\n')
      }
    ];

    const headers = exportConfig.map(c => c.header);
    const processedData = filteredData.map(rawRow => exportConfig.map(config =>
      config.transform ? config.transform(rawRow, cols) : (rawRow[cols[config.colKey]] || '')
    ));

    sheet.getRange(1, 1, 1, headers.length)
      .setValues([headers])
      .setFontWeight("bold")
      .setHorizontalAlignment("center");

    const BATCH_SIZE = CONFIG.BATCH_SIZE;
    for (let i = 0; i < processedData.length; i += BATCH_SIZE) {
      const batch = processedData.slice(i, i + BATCH_SIZE);
      sheet.getRange(i + 2, 1, batch.length, headers.length).setValues(batch);
    }

    sheet.getDataRange()
      .setWrapStrategy(SpreadsheetApp.WrapStrategy.WRAP)
      .setHorizontalAlignment("center")
      .setVerticalAlignment("middle");

    exportConfig.forEach((col, i) => sheet.setColumnWidth(i + 1, col.width));

    sheet.setFrozenRows(1);
    return exportName;

  } catch (error) {
    throw new Error(`Export eșuat: ${error.message}`);
  }
}
function reportPatientData(startDate, endDate) {
  try {
    const startTime = new Date();
    const userEmail = Session.getActiveUser().getEmail();

    const report = {
      codeStats: {
        totalAppearances: 0,
        codeCounts: new Array(1000).fill(0),
      },
      ebCodCounts: new Array(1000).fill(0),
      rpGratuita: 0,
      rpIntegrala: 0,
      btCas: 0,
      btSimplu: 0,
      amScutireAbsenta: 0,
      amScutireSport: 0,
      amAltScop: 0,
      amBursaMedicala: 0,
      aeAvizEpidemiologic: 0,
      ebTotal: 0,
      totalPatients: 0 // This will be set correctly below
    };

    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const sourceSheet = ss.getSheetByName(CONFIG.SHEET_NAME);

    const allHeaders = sourceSheet.getRange(1, 1, 1, sourceSheet.getLastColumn()).getValues()[0];
    const cols = getColumnMappings(allHeaders);

    const lastRow = sourceSheet.getLastRow();
    if (lastRow <= 1) throw new Error("Nu există date despre pacienți disponibile");

    const rawData = sourceSheet.getRange(2, 1, lastRow - 1, sourceSheet.getLastColumn()).getValues();

    const startDateTime = new Date(startDate);
    const endDateTime = new Date(endDate);

    const startRow = findStartRow(rawData, cols.timestamp, startDateTime);
    const endRow = findEndRow(rawData, cols.timestamp, endDateTime);

    const filteredData = (startRow <= endRow) ? rawData.slice(startRow, endRow + 1) : [];

    if (filteredData.length === 0) {
      throw new Error("Nu s-au găsit pacienți în intervalul de date specificat.");
    }

    // CORRECTED: Use filteredData.length instead of rawData.length
    report.totalPatients = filteredData.length;

    filteredData.forEach(row => {
      if (row[cols.codes1]) {
        row[cols.codes1].toString().split(/\s+/).forEach(rawCode => {
          const code = parseInt(rawCode, 10);
          if (!isNaN(code) && code >= 0 && code < 1000) {
            report.codeStats.totalAppearances++;
            report.codeStats.codeCounts[code]++;
          }
        });
      }

      if (row[cols.codes2]) {
        row[cols.codes2].toString().split(/\s+/).forEach(rawEbCod => {
          const ebCod = parseInt(rawEbCod, 10);
          if (!isNaN(ebCod)) report.ebCodCounts[ebCod]++;
        });
      }

      row[cols.rpGratuita] && report.rpGratuita++;
      row[cols.rpIntegrala] && report.rpIntegrala++;
      row[cols.btCas1] && report.btCas++;
      row[cols.btCas2] && report.btCas++;
      row[cols.btCas3] && report.btCas++;
      row[cols.btSimplu] && report.btSimplu++;
      row[cols.amAbsenta] && report.amScutireAbsenta++;
      row[cols.amSport] && report.amScutireSport++;
      row[cols.amAlt] && report.amAltScop++;
      row[cols.amBursa] && report.amBursaMedicala++;
      row[cols.aeAviz] && report.aeAvizEpidemiologic++;
      row[cols.ebInaltime] && report.ebTotal++;
    });

    const uniqueCodes = report.codeStats.codeCounts.filter(count => count > 0).length;

    const ebCodesList = [];
    for (let code = 0; code < 1000; code++) {
      if (report.ebCodCounts[code] > 0) ebCodesList.push({ code, count: report.ebCodCounts[code] });
    }

    const duration = (new Date() - startTime) / 1000;

    const htmlBody = `
      <h2>Raport Registru Medical</h2>
      <p><strong>Număr total de pacienți în interval:</strong> ${report.totalPatients}</p>
      <h3>Coduri de boală:</h3>
      <ul>
        <li>Coduri unice: ${uniqueCodes}</li>
        <li>Total apariții coduri: ${report.codeStats.totalAppearances}</li>
        ${report.codeStats.codeCounts
        .map((count, code) => count > 0 ? `<li>${code}: ${count}</li>` : '')
        .join('')}
      </ul>
      <h3>EB Coduri de boală:</h3>
      <ul>${ebCodesList.length > 0 ? ebCodesList.map(item => `<li>${item.code}: ${item.count}</li>`).join('') : '<li>Niciun cod EB utilizat</li>'}</ul>
      <h3>Tipuri de prescripții:</h3>
      <ul>
        <li>RP gratuite: ${report.rpGratuita}</li>
        <li>RP integrale: ${report.rpIntegrala}</li>
        <li>BT CAS (total 1-3): ${report.btCas}</li>
        <li>BT simple: ${report.btSimplu}</li>
        <li>AM scutiri absențe: ${report.amScutireAbsenta}</li>
        <li>AM scutiri sport: ${report.amScutireSport}</li>
        <li>AM alte scopuri: ${report.amAltScop}</li>
        <li>AM burse medicale: ${report.amBursaMedicala}</li>
        <li>AE avize epidemiologice: ${report.aeAvizEpidemiologic}</li>
        <li>EB examene bilanț: ${report.ebTotal}</li>
      </ul>
      <p>Generat în ${duration} secunde</p>`;

    MailApp.sendEmail({
      to: userEmail,
      subject: `Raportări UMF Registru Medical (${parseDateFromSheet(startDate, true)} - ${parseDateFromSheet(endDate, true)})`,
      htmlBody: htmlBody,
      noReply: true
    });

    return report;

  } catch (error) {
    throw new Error("Raport eșuat: " + error.message);
  }
}