from pathlib import Path
root=Path(__file__).resolve().parent
p=root/'styles.css';s=p.read_text(encoding='utf-8');s=s.replace('transition:width .3s','transition:transform .3s;transform-origin:left');p.write_text(s,encoding='utf-8')
p=root/'app.js';s=p.read_text(encoding='utf-8');s=s.replace('style="width:${job.progress}%"','style="transform:scaleX(${job.progress/100})"');p.write_text(s,encoding='utf-8')
print('Applied layout and progress-animation fixes.')
