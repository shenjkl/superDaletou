---
name: 大乐透研究台
description: 面向个人研究的浅色工作台视觉系统；记录本地交互原型的实际实现
colors:
  bg: "#f3f6fa"
  surface: "#fff"
  ink: "#1c2d46"
  muted: "#607087"
  line: "#e1e7ef"
  primary: "#365dca"
  primary-soft: "#eef2ff"
  teal: "#246e64"
  teal-soft: "#eaf4f0"
  amber: "#8a5a1e"
  amber-soft: "#fff4e3"
  red: "#ad4149"
  red-soft: "#fff0f1"
  focus: "#90aaff"
  primary-hover: "#294db4"
typography:
  headline:
    fontFamily: "'Segoe UI', 'Microsoft YaHei UI', 'PingFang SC', sans-serif"
    fontSize: "27px"
    fontWeight: 650
    lineHeight: 1.4
    letterSpacing: "-.025em"
  title:
    fontFamily: "'Segoe UI', 'Microsoft YaHei UI', 'PingFang SC', sans-serif"
    fontSize: "17px"
    fontWeight: 650
    lineHeight: 1.6
  body:
    fontFamily: "'Segoe UI', 'Microsoft YaHei UI', 'PingFang SC', sans-serif"
    fontSize: "14px"
    lineHeight: 1.6
  label:
    fontFamily: "'Segoe UI', 'Microsoft YaHei UI', 'PingFang SC', sans-serif"
    fontSize: "12px"
    lineHeight: 1.6
  mono:
    fontFamily: "Consolas, 'Cascadia Code', monospace"
    fontSize: "12px"
    lineHeight: 1.6
rounded:
  panel: "12px"
  nav: "8px"
  button: "7px"
  field: "6px"
  tag: "4px"
spacing:
  grid: "20px"
  panel-body: "22px"
  workspace-inline: "34px"
  mobile-inline: "17px"
components:
  button-primary:
    backgroundColor: "{colors.primary}"
    textColor: "{colors.surface}"
    rounded: "{rounded.button}"
    padding: "8px 14px"
    height: "37px"
  button-primary-hover:
    backgroundColor: "{colors.primary-hover}"
  button-secondary:
    backgroundColor: "{colors.surface}"
    textColor: "{colors.ink}"
    rounded: "{rounded.button}"
    padding: "8px 14px"
    height: "37px"
  field:
    backgroundColor: "{colors.surface}"
    textColor: "{colors.ink}"
    rounded: "{rounded.field}"
    padding: "7px 10px"
    height: "39px"
  panel:
    backgroundColor: "{colors.surface}"
    rounded: "{rounded.panel}"
  tag:
    backgroundColor: "#edf1f6"
    textColor: "#53637a"
    rounded: "{rounded.tag}"
    padding: "2px 7px"
  nav-active:
    backgroundColor: "{colors.primary-soft}"
    textColor: "{colors.primary}"
    rounded: "{rounded.nav}"
    padding: "11px 13px"
---

# Design System: 大乐透研究台

## Overview

**Creative North Star: "研究工作台"**

本文件按已实现的静态 HTML/CSS/JavaScript 扫描记录，依据为 `prototype/styles.css`、`prototype/index.html` 的合同、`prototype/app.js` 及桌面与手机概览截图。它描述当前原型的视觉事实，不代表生产框架或真实训练能力已经落地。名称是对既有工作台方向的描述，不新增品牌承诺。

整体是浅灰蓝工作面、白色台账和深蓝文字的轻量操作环境。主操作集中用钴蓝，核验状态用青绿，待确认用赭色，错误用暗红。信息密度适合个人重复查看；边框、间距、标题和状态文字共同建立层级。

**Key Characteristics:**

- 浅色固定导航与白色工作容器，细分隔线组织内容。
- 中文系统字体、紧凑说明文字、对齐的数字及可换行的摘要哈希。
- 状态保留文字标签；真实快照、历史探索与交互演示可辨认。
- 深度主要由平面色块与边框表达，遮罩和抽屉才使用明显投影。

## Colors

色板以冷灰蓝中性色为底，用较少的高饱和操作色标记可行动的位置。精确值以前置 token 为准，角色沿用源码名称。

### Primary

- **钴蓝**（primary）：主按钮、链接、活动导航、当前步骤与进度条。
- **浅钴蓝纸面**（primary-soft）：活动导航、今天日期、操作图标悬停底色与蓝色标签。
- **深钴蓝**（primary-hover）：主按钮悬停。
- **浅蓝焦点**（focus）：可交互控件的键盘焦点轮廓。

### Secondary

- **核验青绿**（teal / teal-soft）：已完成的阶段节点、核验标记与真实快照标签。表示流程或核验状态，不能单独充当研究有效性证明。
- **待确认赭色**（amber / amber-soft）：未证实、待确认与需要解释的边界。

### Tertiary

- **错误暗红**（red / red-soft）：校验失败、负收益文字与错误提示面。

### Neutral

- **冷灰蓝工作面**（bg）：页面背景。
- **白色台账**（surface）：面板、抽屉、输入框和侧栏。
- **墨蓝**（ink）：主要文字、标题、数字。
- **灰蓝说明**（muted）：帮助文案、日期、字段说明与次级内容。
- **冷灰分隔**（line）：面板轮廓、表格行、章节和阶段结构。

**The 状态可读 Rule.** 当前实现同时提供状态文字与色彩；新增状态也要保留可读文字，不能只靠圆点或颜色表达。

## Typography

界面字体使用前置 token 中的系统栈；字体是否存在由运行环境决定，没有加载 Web 字体。哈希和日志使用等宽字体，数值比较使用 `tabular-nums`。这是操作界面的紧凑层级，没有独立的宣传型 Display 字体。

- **Headline**：页标题；标准桌面使用 headline token。小于等于 1150px 为 24px，小于等于 920px 为 23px，手机恢复 24px。
- **Title**：面板标题使用 title token；三级标题为 14px、650，局部节标题为 16px。
- **Body**：根字号为 body token；原型大量说明文字为 11–12px，表格及表单以 12px 为主。保留密度，不将全部正文误写成 14px。
- **Label**：按钮 12px、600；标签 11px；侧栏默认 13px、活动项 650；辅助小字可低至 10px。
- **Mono / 数字**：哈希 12px，日志 11px；表格数值右对齐并使用等宽数字。摘要使用任意位置换行避免撑宽抽屉。
- 页标题说明最大宽度 72ch，中等屏收为 52ch；空状态文案最大 36ch。

## Layout

基础桌面壳为固定左导航（224px）与右侧工作区；工作区相应左移 224px。顶栏高 70px，主内容最大宽 1560px，默认左右内距 34px，上内距 31px。顶栏与内容随页面滚动，源码没有将顶栏设为 sticky。

通用主辅布局是弹性主列加 300px 辅列，列间距 20px；等分双列同样使用 20px。面板通常有带底部分隔线的标题区和 22px 内容内距。留白节奏还包括 25px 标题间隔、22px 章节间隔、16px 表单列间隔；它是实现中的实用尺度，并非统一 8px 倍数体系。

- **1500px 及以上**：主区上内距 39px，辅列增至 340px。
- **1150px 及以下**：侧栏 200px，主区左右内距 24px，辅列 270px、间距 16px，面板内容 18px；隐藏顶栏快照标签。
- **920px 及以下**：主辅与等分双列均转单列，侧栏 186px，顶栏 61px。
- **680px 及以下**：侧栏移出左侧，以菜单按钮打开 224px 导航；工作区左边距清零。顶栏 59px，主区左右内距 17px。标题和操作分行，面板内容左右内距 16px。普通表单转单列，抽屉中标记保留双列的字段继续双列。任务行隐藏辅助图标。表格容器保留横向滚动；不压扁数字列。
- 抽屉宽 510px、最大宽 100%，内容区纵向滚动；手机以 20px 内距适配。

研究日历是本轮概览首屏的具体组织方式；该表面选择和种子见 `prototype/BRIEF.md`，不能据此要求未来每个页面都必须采用日历。已核看 1440px 桌面和 390px 手机概览截图。

## Elevation & Depth

常态面板平放，通过白色表面、细边框和背景色差分层。主要阴影词汇仅用于浮层：全局阴影 `0 14px 46px #1c2d4612` 出现在 toast 和手机侧栏；右侧抽屉为 `-12px 0 48px #15243c1c`。遮罩使用 `#17284055` 及 2px 背景模糊。当前阶段节点还有一圈 `0 0 0 3px #e6edfc` 状态强调。完整数值保存在 sidecar 的扩展字段。

**The 平放面板 Rule.** 面板默认依靠边框和色差；不要给现有普通内容容器增加悬浮阴影来替代其层级。

## Shapes

面板使用较柔和的 12px 圆角；导航项、提示框和日历日期为 8px，按钮为 7px，输入框为 6px，状态标签为 4px。日期小块和标志底板局部使用 9px。圆形仅用于头像、阶段点、状态点与号码球，不将所有操作改成胶囊按钮。

面板和表格主要使用 1px 边框；活动 tab 下划线和向导步骤线使用 2px。图标为内联描边 SVG，导航 19px、按钮 15px、常规图标按钮内 18px。品牌由微倾斜的点阵与中文名称构成。

## Components

### Buttons

主按钮钴蓝底、白字；次按钮白底、浅灰蓝边框。两者使用 button 圆角、8px 14px 内距、最小高度 37px，字号 12px、600。次按钮悬停为淡灰蓝，主按钮悬停加深。文字按钮使用蓝色及 5px 纵向内距；图标按钮为 30px 方形、6px 圆角。

全局交互焦点为 3px 实线轮廓并偏移 3px；禁用按钮透明度 .5，光标为不可操作。源码没有通用按钮按压动画或通用悬停位移动画。

### Chips

默认灰色标签以及蓝、青绿、赭、红四种语义变体，均为 2px 7px 内距、11px、4px 圆角。它们主要是状态说明，并非所有标签都可点击；不要凭视觉将状态标签解释为筛选器。

### Cards / Containers

白色面板、1px 分隔边框、panel 圆角；标题区默认 20px 22px、内容区 22px。说明提示使用蓝色浅底或赭色浅底，错误提示为浅红底；它们承载规则与证据边界。证据不足时使用文字空状态和空白占位线，未绘制虚构表现曲线。

### Inputs / Fields

输入与选择框高 39px、6px 圆角、1px 灰蓝边框、7px 10px 内距。标签 12px、600，帮助文字 11px。只读输入使用浅灰底，复选框使用钴蓝强调。错误通过红色文字、错误提示区域和 `role="alert"` 呈现；没有通用错误边框变体。焦点沿用全局轮廓。

### Navigation

左侧七项导航使用 SVG 加文字，活动项以浅蓝底、蓝字和较粗字重标识，悬停使用浅灰底。前瞻实验计数是小型徽标。页面切换更新 hash、标题和活动项；手机菜单的打开状态由 `aria-expanded` 表示，遮罩和 Escape 可关闭。

页内比较使用带蓝色底线的 tab；奖金规则切换使用普通/主按钮外观。具体键盘行为以当前原型实现为准，本文件不声明未核实的方向键 tab 模式。

### Calendar / Stage Line

七列日期条将快照日以浅蓝实底表示，目标日期使用描边与小点。下方日期块、实验标题、四阶段线和事实行构成研究日程。已完成阶段为青绿，当前阶段为钴蓝且带光圈，未来阶段为灰色；每段均有状态文字。手机仍保留七列和四阶段结构，缩小字号与间距。

### Tables / Research Results

表头浅灰底、11px，表体 12px，行分隔且悬停略提亮；数字列右对齐并保持不换行。柱形图是已有历史探索数据的呈现，图例和数值文字说明统计含义；模型训练完成不自动产生指标。

### Drawer / Feedback / Prototype States

模型训练的简短方法说明使用原生 details 行内展开，同组一次展开一项；树模型和 LSTM 在对应模型行下方显示双列表单，手机改为单列。新建训练任务复用行内入口，取消或 Escape 关闭后恢复焦点。训练进度只更新任务区，不覆盖正在编辑的表单。回测配置在页面顶部展开。训练页辅列按内容高度排列，不随表单拉长。

右侧抽屉保留用于只读摘要。实验草稿在前瞻实验页内编辑：顶部三步进度，主区填写或复核，桌面辅区展示实验摘要，手机按单列排列。步骤切换保留输入，取消回到入口页面，保存后回到草稿列表。打开后聚焦关闭按钮，Tab 在抽屉内循环；关闭后将焦点还给原控件（仍存在时）。Escape、遮罩与关闭按钮均可关闭，页面滚动在打开期间锁定。toast 为底部墨蓝提示，手机居中。

训练演示覆盖未开始、运行中、取消与完成；进度每 700ms 增加 25%，只模拟界面状态。草稿仅保存在内存，刷新清空。截止期、目标期和回测范围提供校验反馈。方案比较的组合数、实际成本与概率已接本地计算；显式重新计算按钮控制任务，修改参数后旧结果失效。精确结果、随机模拟、抽样区间和历史奖金情景分别标记。计算在本页创建的Worker中执行，不改写研究记录。刷新演示仍使用静态快照。不存在真实联网训练、购票、封存、研究写入或后台定时任务。侧栏位移使用 .18s 过渡，进度条声明 .3s transform 过渡；`prefers-reduced-motion` 禁用过渡。

## Do's and Don'ts

### Do:

- **Do** 沿用前置 token 与现有系统字体栈，使用文字解释每种研究状态。
- **Do** 为真实快照、历史探索和演示操作保留清楚的标签及时间边界。
- **Do** 让比较表格的数字右对齐，并在窄屏通过横向滚动保留列结构。
- **Do** 对新操作延续可见焦点、表单标签与明确错误文字。

### Don't:

- **Don't** 把演示完成、已核验或绿色状态表达为已经证明盈利或优于随机。
- **Don't** 用虚构收益曲线填充尚无结算结果的空状态。
- **Don't** 将状态标签一律处理为可点击筛选器，或为普通面板添加额外浮动阴影。
- **Don't** 把当前概览的研究日历构图、静态数据日期或原型框架推广为全局品牌约束。


## 操作文案

按用户要求，常规页面、配置表单、按钮、任务状态和提示均采用产品用语，不出现“演示训练”“示例配置”等标签。能力范围集中在“使用说明”：任务流程尚未接入训练和回测服务，不生成模型评估结果。已有研究事实、未结算状态和历史奖金情景说明继续保留。

计算结果区使用紧凑标题操作栏，模拟次数和唯一重新计算入口并列，按钮图标固定15px；未计算或参数失效时显示简短状态行，避免大型空状态和重复按钮。

方案结果采用单一图表明细表：每方案一行，中奖与盈利单元格合并概率数值、共同0—100%刻度条和95%模拟区间，保留成本、计算方式、期望毛奖金、一等奖概率与不参与基准。手机在表格内部横向滚动，不撑宽页面。

方案生成模块：随机、热号与冷号方法基于已核验官方历史窗口在本地生成合法去重组合，支持单式、复式与固定4胆6拖/后3结构；同时生成独立种子的同成本同结构随机对照。树模型与LSTM缺少推理权重时阻止生成。输入变更后旧结果失效。候选仅标为待评估，不提供采用排名。登记草稿携带方法版本、历史摘要、参数、目标期、时间及候选/对照全部号码，组合包各1份；在当前会话保存，不改写既有封存台账。
