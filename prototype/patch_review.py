from pathlib import Path
root=Path(__file__).resolve().parent
p=root/'app.js';s=p.read_text(encoding='utf-8')
s=s.replace('=>`<button class="button ${type}" data-action=', '=>`<button type="button" class="button ${type}" data-action=')
s=s.replace("state.budget=Math.max(6,Math.min(360,Number(t.value)||36));render=true;", "const requested=Number(t.value)||36;state.budget=Math.max(6,Math.min(360,Math.floor(requested/6)*6));if(state.budget!==requested)toast('为保持等成本对照，预算按 6 元步长调整为 '+state.budget+' 元。');render=true;")
s=s.replace("&&cut<target;el('leak-result')", "&&cut<target&&cut<=D.issue;el('leak-result')")
s=s.replace("'时间边界未通过：训练截止期必须早于目标期，且均为五位期号。'", "cut>D.issue?'训练数据超出快照：当前数据只到第 '+D.issue+' 期。':'时间边界未通过：训练截止期必须早于目标期，且均为五位期号。'")
p.write_text(s,encoding='utf-8')
p=root/'check_prototype.cjs';s=p.read_text(encoding='utf-8')
s=s.replace("await page.getByRole('button',{name:'恢复 36 元预算'}).click();await page.screenshot", "await page.getByRole('button',{name:'恢复 36 元预算'}).click();await page.locator('#budget').fill('35');await page.locator('#budget').dispatchEvent('change');assert.equal(await page.locator('#budget').inputValue(),'30');await page.getByRole('button',{name:'恢复 36 元预算'}).click();await page.screenshot")
s=s.replace("await page.locator('#target-check').fill('26124');assert((await page.locator('#leak-result').innerText()).includes('通过'));", "await page.locator('#target-check').fill('26124');assert((await page.locator('#leak-result').innerText()).includes('通过'));await page.locator('#cutoff-check').fill('26124');await page.locator('#target-check').fill('26125');assert((await page.locator('#leak-result').innerText()).includes('超出快照'));await page.getByRole('button',{name:'配置回测演示'}).click();await page.locator('#drawer-content').getByRole('button',{name:'取消',exact:true}).click();assert(await page.locator('#drawer').isHidden());assert(await page.locator('#toast').isHidden());")
# The prior budget adjustment toast may still be visible; distinguish unwanted backtest success from an existing harmless toast.
s=s.replace("assert(await page.locator('#toast').isHidden());", "assert(!(await page.locator('#toast').innerText()).includes('演示配置校验通过'));")
s=s.replace("await page.getByRole('button',{name:'新建训练任务',exact:true}).click();await page.locator('#train-cutoff').fill('26124');", "await page.getByRole('button',{name:'新建训练任务',exact:true}).click();await page.locator('#drawer-content').getByRole('button',{name:'取消',exact:true}).click();assert(await page.locator('#drawer').isHidden());await page.getByRole('heading',{name:'还没有训练任务',exact:true}).waitFor();await page.getByRole('button',{name:'新建训练任务',exact:true}).click();await page.locator('#train-cutoff').fill('26124');")
s=s.replace("await page.getByRole('button',{name:'新建实验草稿',exact:true}).click();await page.locator('#exp-name').fill", "await page.getByRole('button',{name:'新建实验草稿',exact:true}).click();await page.locator('#drawer-content').getByRole('button',{name:'取消',exact:true}).click();assert(await page.locator('#drawer').isHidden());assert(!(await page.locator('#main').innerText()).includes('演示草稿已保存'));await page.getByRole('button',{name:'新建实验草稿',exact:true}).click();await page.locator('#exp-name').fill")
s=s.replace("'mobile-navigation'],browser_errors", "'mobile-navigation','three-form-cancel-no-submit','budget-step-alignment','snapshot-cutoff-limit'],browser_errors")
p.write_text(s,encoding='utf-8')
print('Patched the three material review findings and regression checks.')
