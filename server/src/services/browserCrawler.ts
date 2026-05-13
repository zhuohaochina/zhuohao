/**
 * 浏览器采集服务
 * 使用 Puppeteer 模拟真实浏览器，绕过 TLS 指纹检测
 * 专门用于 push2.eastmoney.com 等有反爬限制的接口
 */
import puppeteer, { Browser, Page } from 'puppeteer';
import axios from 'axios';

/**
 * 浏览器采集服务类
 */
export class BrowserCrawlerService {
  private browser: Browser | null = null;
  private page: Page | null = null;
  private initialized: boolean = false;
  private readonly maxRequestRetries: number = 5;
  private readonly retryBaseDelayMs: number = 1800;
  private readonly betweenPageDelayMs: number = 800;
  private readonly cooldownEveryPages: number = 20;
  private readonly cooldownDelayMs: number = 3000;

  /**
   * 初始化浏览器
   */
  async init(): Promise<void> {
    if (this.browser && this.initialized) return;

    console.log('[BrowserCrawler] 启动浏览器...');
    this.browser = await puppeteer.launch({
      headless: true,  // 无头模式，不显示浏览器窗口
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-dev-shm-usage',
        '--disable-accelerated-2d-canvas',
        '--disable-gpu',
        '--window-size=1920,1080'
      ]
    });

    this.page = await this.browser.newPage();

    // 设置 User-Agent
    await this.page.setUserAgent(
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    );

    // 设置视口
    await this.page.setViewport({ width: 1920, height: 1080 });

    // 先访问东方财富行情页面，建立 cookie 和 session
    console.log('[BrowserCrawler] 访问东方财富页面建立会话...');
    await this.page.goto('https://quote.eastmoney.com/center/gridlist.html', {
      waitUntil: 'networkidle2',
      timeout: 30000
    });

    this.initialized = true;
    console.log('[BrowserCrawler] 浏览器初始化完成');
  }

  /**
   * 关闭浏览器
   */
  async close(): Promise<void> {
    if (this.browser) {
      await this.browser.close();
      this.browser = null;
      this.page = null;
      this.initialized = false;
      console.log('[BrowserCrawler] 浏览器已关闭');
    }
  }

  /**
   * 使用浏览器环境发送 GET 请求
   * @param url 请求 URL
   * @param params 查询参数
   * @returns 响应数据
   */
  async fetchWithBrowser(url: string, params: Record<string, any> = {}): Promise<any> {
    await this.init();

    if (!this.page) {
      throw new Error('浏览器页面未初始化');
    }

    // 构建完整 URL
    const searchParams = new URLSearchParams();
    for (const [key, value] of Object.entries(params)) {
      searchParams.append(key, String(value));
    }
    const fullUrl = `${url}?${searchParams.toString()}`;

    console.log('[BrowserCrawler] 请求:', fullUrl.substring(0, 100) + '...');

    // 在浏览器环境中执行 fetch
    const result = await this.page.evaluate(async (fetchUrl: string) => {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 20000);

      try {
        const response = await fetch(fetchUrl, {
          method: 'GET',
          credentials: 'include',
          signal: controller.signal,
          headers: {
            'Accept': '*/*',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8'
          }
        });

        if (!response.ok) {
          const text = await response.text();
          return {
            success: false,
            error: `HTTP ${response.status}`,
            status: response.status,
            responsePreview: text.slice(0, 120)
          };
        }

        const text = await response.text();
        return { success: true, data: text };
      } catch (error: any) {
        return { success: false, error: error.message };
      } finally {
        clearTimeout(timeoutId);
      }
    }, fullUrl);

    if (!result.success) {
      const preview = result.responsePreview ? `, body: ${result.responsePreview}` : '';
      throw new Error(`浏览器请求失败: ${result.error}${preview}`);
    }

    return result.data;
  }

  /**
   * 使用 Node HTTP 请求抓取（优先路径，稳定性更高）
   */
  private async fetchWithHttp(url: string, params: Record<string, any> = {}): Promise<string> {
    const response = await axios.get(url, {
      params,
      timeout: 20000,
      responseType: 'text',
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': 'https://quote.eastmoney.com/center/gridlist.html',
        'Accept': '*/*',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8'
      }
    });
    return String(response.data ?? '');
  }

  /**
   * 采集东方财富 A股列表 (stock_jsonp)
   * @param params API 参数
   * @param pageNum 页码
   * @param pageSize 每页数量
   * @returns 解析后的数据
   */
  async fetchStockList(
    params: Record<string, any>,
    pageNum: number = 1,
    pageSize: number = 5000,
    onRetry?: (message: string) => void
  ): Promise<{ data: any[]; total: number }> {
    const url = 'https://82.push2.eastmoney.com/api/qt/clist/get';

    const fullParams = {
      ...params,
      cb: 'jQuery',
      pn: pageNum,
      pz: pageSize
    };

    let lastError: Error | null = null;

    for (let attempt = 1; attempt <= this.maxRequestRetries; attempt++) {
      try {
        let responseText: string;
        try {
          responseText = await this.fetchWithHttp(url, fullParams);
        } catch (httpError: any) {
          const message = httpError?.message || String(httpError);
          console.warn(`[BrowserCrawler] HTTP请求失败，回退浏览器模式: ${message}`);
          responseText = await this.fetchWithBrowser(url, fullParams);
        }
        const trimmedText = String(responseText || '').trim();

        if (trimmedText.startsWith('<!DOCTYPE') || trimmedText.startsWith('<html')) {
          throw new Error('返回了 HTML 页面，可能触发风控或限流');
        }

        // 解析 JSONP 响应（兼容不同 callback 名称）
        const jsonStr = trimmedText
          .replace(/^[^(]+\(/, '')
          .replace(/\)\s*;?$/, '');

        const jsonData = JSON.parse(jsonStr);

        if (!jsonData.data || !jsonData.data.diff) {
          return { data: [], total: 0 };
        }

        return {
          data: jsonData.data.diff,
          total: jsonData.data.total || 0
        };
      } catch (error) {
        lastError = error as Error;
        const errorMessage = lastError?.message || String(error);
        const shouldRetry = this.shouldRetryStockRequest(errorMessage);

        if (!shouldRetry || attempt >= this.maxRequestRetries) {
          break;
        }

        const delayMs = this.retryBaseDelayMs * attempt + Math.floor(Math.random() * 500);
        const retryMsg = `第 ${pageNum} 页请求失败（${attempt}/${this.maxRequestRetries}）：${errorMessage}，${Math.ceil(delayMs / 1000)} 秒后重试...`;
        console.warn(`[BrowserCrawler] ${retryMsg}`);
        onRetry?.(retryMsg);

        await this.resetBrowserSession();
        await this.sleep(delayMs);
      }
    }

    const finalErrorMessage = lastError?.message || '未知错误';
    throw new Error(`第 ${pageNum} 页请求失败（已重试 ${this.maxRequestRetries} 次）: ${finalErrorMessage}`);
  }

  /**
   * 判断错误是否值得重试
   */
  private shouldRetryStockRequest(errorMessage: string): boolean {
    const normalized = errorMessage.toLowerCase();
    return (
      normalized.includes('failed to fetch') ||
      normalized.includes('timeout') ||
      normalized.includes('network') ||
      normalized.includes('econnreset') ||
      normalized.includes('econnrefused') ||
      normalized.includes('err_empty_response') ||
      normalized.includes('empty response') ||
      normalized.includes('target closed') ||
      normalized.includes('session closed') ||
      normalized.includes('navigation failed') ||
      normalized.includes('风控') ||
      normalized.includes('限流') ||
      normalized.includes('http 4') ||
      normalized.includes('http 5')
    );
  }

  /**
   * 重置浏览器会话（用于失败后恢复）
   */
  private async resetBrowserSession(): Promise<void> {
    try {
      await this.close();
    } catch (error) {
      console.warn('[BrowserCrawler] 重置会话时关闭浏览器失败:', error);
    }
  }

  /**
   * 采集东方财富 A股列表全部数据（自动分页）
   * @param params API 参数
   * @param onProgress 进度回调
   * @returns 所有数据
   */
  async fetchAllStockList(
    params: Record<string, any>,
    onProgress?: (current: number, total: number, message: string) => void
  ): Promise<any[]> {
    const allData: any[] = [];
    let page = 1;
    let totalPages = 1;
    let pageSize = 5000;
    let actualPageSize = 0;

    try {
      while (page <= totalPages) {
        onProgress?.(page, totalPages, `正在获取第 ${page}/${totalPages} 页...`);

        const result = await this.fetchStockList(
          params,
          page,
          pageSize,
          (retryMessage: string) => onProgress?.(page, totalPages, retryMessage)
        );

        if (result.data.length === 0) break;

        // 第一次请求时检测实际每页数量
        if (page === 1) {
          actualPageSize = result.data.length;
          if (actualPageSize < pageSize && actualPageSize > 0) {
            console.log(`[BrowserCrawler] API 限制每页最多 ${actualPageSize} 条`);
            pageSize = actualPageSize;
          }
          totalPages = Math.ceil(result.total / pageSize);
        }

        allData.push(...result.data);

        console.log(`[BrowserCrawler] 第 ${page}/${totalPages} 页: ${result.data.length} 条，累计 ${allData.length}/${result.total}`);

        onProgress?.(page, totalPages, `第 ${page}/${totalPages} 页完成，累计 ${allData.length} 条`);

        page++;

        // 请求间隔
        if (page <= totalPages) {
          const pageDelay = this.betweenPageDelayMs + Math.floor(Math.random() * 500);
          await this.sleep(pageDelay);

          if ((page - 1) > 0 && (page - 1) % this.cooldownEveryPages === 0) {
            onProgress?.(page - 1, totalPages, `已连续请求 ${(page - 1)} 页，冷却 ${Math.ceil(this.cooldownDelayMs / 1000)} 秒...`);
            await this.sleep(this.cooldownDelayMs);
          }
        }
      }

      console.log(`[BrowserCrawler] 采集完成，共 ${allData.length} 条`);
      return allData;

    } catch (error) {
      console.error('[BrowserCrawler] 采集失败:', error);
      throw error;
    }
  }

  /**
   * 延时函数
   */
  private sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}

// 单例
export const browserCrawlerService = new BrowserCrawlerService();
