const axios = require('axios');
const cheerio = require('cheerio');

// 설정
const CONFIG = {
    USER_AGENT: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    BASE_URL: 'https://www.bntnews.co.kr',
    SEARCH_PATH: '/article/search?searchText=%ED%86%A0%EC%8A%A4',
    TIMEOUT: 10000,
    WEBHOOK_URL: process.env.DISCORD_WEBHOOK_URL
};

// 날짜 관련 유틸리티
function isValidDate(dateString) {
    const date = new Date(dateString);
    return date instanceof Date && !isNaN(date);
}

function parseDate(dateString) {
    if (!dateString) return new Date();
    
    if (/^\d{4}-\d{2}-\d{2}$/.test(dateString)) {
        const date = new Date(dateString);
        if (isValidDate(date)) return date;
    }
    
    console.log('잘못된 날짜 형식입니다. YYYY-MM-DD 형식(예: 2025-07-24)을 사용해주세요.');
    return new Date();
}

function getKoreanDate(date = new Date()) {
    const month = date.getMonth() + 1;
    const day = date.getDate();
    return `${month}월 ${day}일`;
}

// 웹 스크래핑
async function scrapeQuizAnswers(targetDate = new Date()) {
    try {
        const targetKorean = getKoreanDate(targetDate);
        console.log(`검색 날짜: ${targetKorean}`);

        // bnt뉴스 토스 검색 페이지 가져오기
        const response = await axios.get(`${CONFIG.BASE_URL}${CONFIG.SEARCH_PATH}`, {
            headers: { 'User-Agent': CONFIG.USER_AGENT },
            timeout: CONFIG.TIMEOUT
        });

        const $ = cheerio.load(response.data);
        
        // 지정된 날짜 기사 링크 찾기
        let targetArticleLink = null;
        $('a').each((_, element) => {
            const text = $(element).text().trim();
            if (text.includes('토스 행운퀴즈 정답') && text.includes(targetKorean)) {
                const href = $(element).attr('href');
                if (href) {
                    targetArticleLink = href.startsWith('http') ? href : `${CONFIG.BASE_URL}${href}`;
                    return false; // break
                }
            }
        });

        if (!targetArticleLink) {
            console.log(`${targetKorean} 날짜의 토스 행운퀴즈 기사를 찾을 수 없습니다.`);
            return null;
        }

        console.log('기사 링크:', targetArticleLink);

        // 기사 페이지 가져오기
        const articleResponse = await axios.get(targetArticleLink, {
            headers: { 'User-Agent': CONFIG.USER_AGENT },
            timeout: CONFIG.TIMEOUT
        });

        const $article = cheerio.load(articleResponse.data);
        
        // 기사 제목과 내용 파싱
        const title = $article('h1, .title, .article-title, .headline').first().text().trim() || 
                     $article('title').text().trim();
        
        const content = $article('.article-content, .content, article, .article-body, .text').html() || 
                       $article('body').html();

        return { title, content, url: targetArticleLink };

    } catch (error) {
        console.error('스크래핑 중 오류 발생:', error.message);
        return null;
    }
}

// 퀴즈 정답 파싱
function parseQuizAnswers(content) {
    const quizData = [];
    const $ = cheerio.load(content);
    
    let currentTitle = '';
    
    // <strong> 태그들을 모두 찾아서 처리
    $('strong').each((_, element) => {
        const html = $(element).html();
        const text = $(element).text().trim();
        
        // 정답 패턴인지 확인
        if (text.includes('정답 -') || text.includes('정답:')) {
            // HTML에서 <br /> 태그를 기준으로 분리
            const parts = html.split(/<br\s*\/?>/gi);
            
            parts.forEach(part => {
                const cleanPart = part.replace(/<[^>]*>/g, '').trim();
                
                if (cleanPart.includes('정답 -') || cleanPart.includes('정답:')) {
                    // 정답 부분
                    const answer = cleanPart.replace(/정답\s*[-:]\s*/, '').trim();
                    if (currentTitle && answer) {
                        quizData.push({
                            title: currentTitle,
                            answer: answer
                        });
                        currentTitle = '';
                    }
                } else if (cleanPart && cleanPart.length > 2 && 
                          !cleanPart.includes('정답') && 
                          !cleanPart.includes('토스') && 
                          !cleanPart.includes('■') &&
                          !cleanPart.includes('bnt뉴스')) {
                    // 제목 부분
                    currentTitle = cleanPart;
                }
            });
        } else {
            // 정답이 아닌 경우 제목으로 간주
            if (text.length > 2 && 
                !text.includes('정답') && 
                !text.includes('토스') && 
                !text.includes('■') &&
                !text.includes('bnt뉴스')) {
                currentTitle = text;
            }
        }
    });
    
    return quizData;
}

// Discord 메시지 전송
async function sendToDiscord(title, quizData, targetDate = new Date(), articleData = {}) {
    if (!CONFIG.WEBHOOK_URL) {
        console.log('Discord Webhook URL이 설정되지 않았습니다.');
        return;
    }

    const targetKorean = getKoreanDate(targetDate);
    
    let message = `🎯 **${title}**\n`;
    message += `🔗 ${articleData.url}\n\n`;
    message += `📅 **${targetKorean} 토스 행운퀴즈 정답**\n\n`;
    
    if (quizData.length > 0) {
        quizData.forEach((quiz, index) => {
            message += `**${index + 1}. ${quiz.title}**\n`;
            message += `└ ${quiz.answer}\n\n`;
        });
    } else {
        message += '❌ 정답을 찾을 수 없습니다.\n';
    }
    
    message += `⏰ 업데이트 시간: ${new Date().toLocaleString('ko-KR')}`;

    try {
        await axios.post(CONFIG.WEBHOOK_URL, { content: message });
        console.log('Discord로 메시지 전송 완료!');
    } catch (error) {
        console.error('Discord 전송 실패:', error.message);
    }
}

// 메인 실행 함수
async function main() {
    const dateArg = process.argv[2];
    const targetDate = parseDate(dateArg);
    
    console.log('토스 퀴즈 정답 스크래핑 시작...');
    if (dateArg) {
        console.log(`지정된 날짜: ${dateArg} -> ${getKoreanDate(targetDate)}`);
    } else {
        console.log('오늘 날짜 사용');
    }
    
    const articleData = await scrapeQuizAnswers(targetDate);
    
    if (articleData) {
        console.log('기사 제목:', articleData.title);
        
        const quizData = parseQuizAnswers(articleData.content);
        console.log('파싱된 퀴즈 데이터:', quizData);
        
        await sendToDiscord(articleData.title, quizData, targetDate, articleData);
    } else {
        console.log('기사를 찾을 수 없거나 오류가 발생했습니다.');
    }
}

// 스크립트 실행
if (require.main === module) {
    main().catch(console.error);
}

module.exports = { main, scrapeQuizAnswers, parseQuizAnswers, sendToDiscord };
