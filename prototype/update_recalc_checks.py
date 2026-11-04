from pathlib import Path
root=Path(__file__).resolve().parent
p=root/'check_prototype.cjs';s=p.read_text(encoding='utf-8')
start=s.index('await page.locator(\'.nav-item[href="#comparison"]\').click();')
end=s.index('await page.locator(\'.nav-item[href="#backtest"]\').click();',start)
new='''await page.locator('.nav-item[href="#comparison"]').click();
await page.locator('#front-count').selectOption('10');await page.locator('#back-count').selectOption('4');assert((await page.locator('.calculator-result').innerText()).includes('3,024'));
await page.locator('#calc-additional').check();assert((await page.locator('.calculator-result').innerText()).includes('4,536'));
await page.locator('#budget').fill('35');await page.locator('#budget').dispatchEvent('change');assert.equal(await page.locator('#budget').inputValue(),'35');assert((await page.locator('#main').innerText()).includes('预算内，余 1 元'));assert((await page.locator('#main').innerText()).includes('预算内，余 2 元'));
await page.locator('#front-count').selectOption('7');await page.locator('#back-count').selectOption('3');await page.locator('#calc-additional').uncheck();await page.getByRole('button',{name:'将 126 元同步为预算',exact:true}).click();assert.equal(await page.locator('#budget').inputValue(),'126');
await page.locator('.calculation-toolbar').getByRole('button',{name:'重新计算',exact:true}).click();await page.getByText('已按当前参数重算',{exact:true}).waitFor();assert((await page.locator('#comparison-results').innerText()).includes('17.40%'));assert((await page.locator('#comparison-results').innerText()).includes('3.12%'));await page.screenshot({path:path.join(out,'comparison-desktop.png'),fullPage:true});
await page.locator('#front-count').selectOption('8');await page.getByRole('heading',{name:'参数已变更，需要重新计算',exact:true}).waitFor();assert.equal(await page.getByText('已按当前参数重算',{exact:true}).count(),0);
'''
s=s[:start]+new+s[end:];s=s.replace("'budget-step-alignment'","'budget-actual-cost'")
s=s.replace("'snapshot-cutoff-limit'],browser_errors", "'snapshot-cutoff-limit','worker-recalculation-126','stale-results-hidden'],browser_errors")
p.write_text(s,encoding='utf-8')
p=root/'check_budget_edges.cjs';s=p.read_text(encoding='utf-8').replace("['35','30']","['35','35']").replace("'35→30'","'35→35 with actual costs'");p.write_text(s,encoding='utf-8')
print('Updated regression checks for actual recalculation and explicit remaining budget.')
