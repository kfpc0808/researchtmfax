const { GoogleSpreadsheet } = require('google-spreadsheet');
const { JWT } = require('google-auth-library');

// 구글 시트 인증 설정
const serviceAccountAuth = new JWT({
  email: process.env.GOOGLE_CLIENT_EMAIL,
  key: process.env.GOOGLE_PRIVATE_KEY.replace(/\\n/g, '\n'),
  scopes: ['https://www.googleapis.com/auth/spreadsheets'],
});

const doc = new GoogleSpreadsheet(process.env.GOOGLE_SHEET_ID, serviceAccountAuth);

// 시간 관련 유틸리티 함수
const getKSTDateTime = () => {
  const now = new Date();
  const utc = now.getTime() + (now.getTimezoneOffset() * 60 * 1000);
  const kstOffset = 9 * 60 * 60 * 1000;
  const kstNow = new Date(utc + kstOffset);
  const year = kstNow.getFullYear();
  const month = ('0' + (kstNow.getMonth() + 1)).slice(-2);
  const day = ('0' + kstNow.getDate()).slice(-2);
  const hours = ('0' + kstNow.getHours()).slice(-2);
  const minutes = ('0' + kstNow.getMinutes()).slice(-2);
  return `${year}-${month}-${day} ${hours}:${minutes}`;
};

const getTodayKST = () => {
  return getKSTDateTime().split(' ')[0];
};

// 메인 핸들러 함수
exports.handler = async (event) => {
  try {
    // 1. 요청 데이터 파싱
    if (!event.body) {
      return { statusCode: 400, body: JSON.stringify({ error: 'Empty body' }) };
    }
    const { action, sheetName, payload = {} } = JSON.parse(event.body);
    const { data, rowIndex, filter, page, limit, userRole, forceSave } = payload;

    // 2. 구글 시트 로드
    await doc.loadInfo();
    const sheet = doc.sheetsByTitle[sheetName];

    if (!sheet) {
      return { statusCode: 400, body: JSON.stringify({ error: `Sheet '${sheetName}' not found.` }) };
    }

    // 3. 액션별 로직 수행
    switch (action) {
      case 'read': {
        const rows = await sheet.getRows();
        let filteredRows = rows;
        
        if (filter && Object.keys(filter).length > 0) {
          filteredRows = rows.filter(row => {
            return Object.keys(filter).every(key => {
              const rowValue = String(row.get(key) || '');
              const filterValue = String(filter[key] || '');
              if (filterValue === '') return true;
              return rowValue.toLowerCase().includes(filterValue.toLowerCase());
            });
          });
        }

        const total = filteredRows.length;
        const currentPage = page || 1;
        const currentLimit = limit || 15;
        const startIndex = (currentPage - 1) * currentLimit;
        const endIndex = currentPage * currentLimit;
        const paginatedRows = filteredRows.slice(startIndex, endIndex);

        const responseData = paginatedRows.map(row => {
          const rowObject = row.toObject();
          // 원본 인덱스 보존 (업데이트 시 필요)
          rowObject.originalIndex = rows.findIndex(r => r.rowNumber === row.rowNumber);
          return rowObject;
        });

        return { statusCode: 200, body: JSON.stringify({ data: responseData, total: total }) };
      }

      case 'readAll': {
        const rows = await sheet.getRows();
        const allData = rows.map((row, index) => ({ ...row.toObject(), originalIndex: index }));
        return { statusCode: 200, body: JSON.stringify(allData) };
      }

      case 'write':
      case 'update': {
        if (!data || (action === 'update' && rowIndex === undefined)) {
          return { statusCode: 400, body: JSON.stringify({ message: 'Data/rowIndex required.' }) };
        }

        const specialistName = data['담당 전문위원'];

        // TMer 전용 비즈니스 로직
        if (sheetName === 'Companies' && userRole === 'TMer' && specialistName) {
          // 중복 연락 체크
          if (!forceSave && (data['메세지생성'] || data['통화내용_TMer'])) {
            const today = getTodayKST();
            const rows = await sheet.getRows();
            const hasContactedToday = rows.some((row, index) => {
              const isDifferentRow = (action === 'update') ? index !== rowIndex : true;
              return isDifferentRow && row.get('담당 전문위원') === specialistName && row.get('lastContactDate') === today;
            });

            if (hasContactedToday) {
              return {
                statusCode: 200,
                body: JSON.stringify({ success: false, confirmation_required: true, message: '해당 전문위원은 오늘 이미 다른 기업에 연락 기록이 있습니다.' })
              };
            }
          }
          data.lastContactDate = getTodayKST();
        }

        // 통화일시 자동 기록
        if (sheetName === 'Companies' && userRole === 'TMer' && data['통화내용_TMer']) {
          const rows = await sheet.getRows();
          if (action === 'update' && rows[rowIndex]) {
            if (rows[rowIndex].get('통화내용_TMer') !== data['통화내용_TMer']) {
              data['통화일시'] = getKSTDateTime();
            }
          } else {
            data['통화일시'] = getKSTDateTime();
          }
        }

        // 시트 반영
        if (action === 'write') {
          await sheet.addRow(data);
        } else {
          const rowsToUpdate = await sheet.getRows();
          if (rowsToUpdate[rowIndex]) {
            Object.keys(data).forEach(key => rowsToUpdate[rowIndex].set(key, data[key]));
            await rowsToUpdate[rowIndex].save();
          }
        }
        return { statusCode: 200, body: JSON.stringify({ success: true }) };
      }

      case 'delete': {
        if (rowIndex === undefined) return { statusCode: 400, body: JSON.stringify({ message: 'rowIndex required.' }) };
        const rows = await sheet.getRows();
        if (rows[rowIndex]) {
          await rows[rowIndex].delete();
          return { statusCode: 200, body: JSON.stringify({ success: true }) };
        }
        return { statusCode: 404, body: JSON.stringify({ error: 'Row not found.' }) };
      }

      default:
        return { statusCode: 400, body: JSON.stringify({ error: 'Invalid action.' }) };
    }
  } catch (error) {
    console.error('Error:', error);
    return { statusCode: 500, body: JSON.stringify({ error: error.message }) };
  }
};