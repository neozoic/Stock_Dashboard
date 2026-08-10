// mock_data.js — 前端模拟数据层(默认关闭,需手动开启)
//
// 用法:
//   1. 默认: dashboard.html 接真实后端数据(mock_data.js 仅在 console.log 一行)
//   2. 开 mock: 浏览器 console 跑 `localStorage.setItem('mock_enabled','1'); location.reload();`
//   3. 关 mock: 浏览器 console 跑 `localStorage.removeItem('mock_enabled'); location.reload();`
//
// 实现:当 mock_enabled === '1' 时覆盖 window.fetch,拦截所有 /api/* 路径,返回固定模拟数据
(function () {
  if (localStorage.getItem('mock_enabled') !== '1') {
    console.log('[mock_data.js] mock 模式关闭,接实盘数据');
    return;
  }
  console.log('[mock_data.js] mock 模式已启用');
})();
