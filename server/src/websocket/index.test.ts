/**
 * WebSocket 服务测试
 * **Feature: cninfo-data-collector, Property 4: 进度消息格式正确性**
 */
import * as fc from 'fast-check';
import { createProgressMessage } from './index';
import { ProgressMessage } from '../models/types';

describe('WebSocket 进度消息', () => {
  /**
   * **Feature: cninfo-data-collector, Property 4: 进度消息格式正确性**
   * **验证：需求 2.3, 7.2**
   * 
   * 对于任意进度推送消息，消息应该包含 type、metadataId、progress、message、timestamp 字段，
   * 且 progress 值应该在 0-100 范围内。
   */
  it('属性测试：进度消息应该包含所有必需字段且 progress 在 0-100 范围内', () => {
    fc.assert(
      fc.property(
        fc.constantFrom('progress', 'complete', 'error') as fc.Arbitrary<ProgressMessage['type']>,
        fc.integer({ min: 1, max: 10000 }),
        fc.integer({ min: -100, max: 200 }), // 测试边界情况
        fc.string({ minLength: 0, maxLength: 100 }),
        (type, metadataId, progress, message) => {
          const result = createProgressMessage(type, metadataId, progress, message);
          
          // 验证：消息包含所有必需字段
          expect(result).toHaveProperty('type');
          expect(result).toHaveProperty('metadataId');
          expect(result).toHaveProperty('progress');
          expect(result).toHaveProperty('message');
          expect(result).toHaveProperty('timestamp');
          
          // 验证：字段类型正确
          expect(typeof result.type).toBe('string');
          expect(typeof result.metadataId).toBe('number');
          expect(typeof result.progress).toBe('number');
          expect(typeof result.message).toBe('string');
          expect(typeof result.timestamp).toBe('number');
          
          // 验证：type 是有效值
          expect(['progress', 'complete', 'error']).toContain(result.type);
          
          // 验证：progress 在 0-100 范围内
          expect(result.progress).toBeGreaterThanOrEqual(0);
          expect(result.progress).toBeLessThanOrEqual(100);
          
          // 验证：metadataId 正确传递
          expect(result.metadataId).toBe(metadataId);
          
          // 验证：message 正确传递
          expect(result.message).toBe(message);
          
          // 验证：timestamp 是合理的时间戳
          expect(result.timestamp).toBeGreaterThan(0);
          expect(result.timestamp).toBeLessThanOrEqual(Date.now() + 1000);
        }
      ),
      { numRuns: 100 }
    );
  });

  /**
   * 单元测试：创建进度消息
   */
  it('应该正确创建进度消息', () => {
    const message = createProgressMessage('progress', 1, 50, '正在处理...');
    
    expect(message.type).toBe('progress');
    expect(message.metadataId).toBe(1);
    expect(message.progress).toBe(50);
    expect(message.message).toBe('正在处理...');
    expect(message.timestamp).toBeDefined();
  });

  /**
   * 单元测试：progress 值被限制在 0-100 范围内
   */
  it('应该将 progress 值限制在 0-100 范围内', () => {
    const messageLow = createProgressMessage('progress', 1, -50, 'test');
    expect(messageLow.progress).toBe(0);
    
    const messageHigh = createProgressMessage('progress', 1, 150, 'test');
    expect(messageHigh.progress).toBe(100);
    
    const messageNormal = createProgressMessage('progress', 1, 75, 'test');
    expect(messageNormal.progress).toBe(75);
  });

  /**
   * 单元测试：创建完成消息
   */
  it('应该正确创建完成消息', () => {
    const message = createProgressMessage('complete', 2, 100, '更新完成');
    
    expect(message.type).toBe('complete');
    expect(message.metadataId).toBe(2);
    expect(message.progress).toBe(100);
    expect(message.message).toBe('更新完成');
  });

  /**
   * 单元测试：创建错误消息
   */
  it('应该正确创建错误消息', () => {
    const message = createProgressMessage('error', 3, 0, '更新失败');
    
    expect(message.type).toBe('error');
    expect(message.metadataId).toBe(3);
    expect(message.progress).toBe(0);
    expect(message.message).toBe('更新失败');
  });
});
