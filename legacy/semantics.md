读取GenBank flat-file的VERSION、LOCUS长度/linear|circular、FEATURES与ORIGIN；可用BioPython解析但不得让库自动截断越界坐标后当合法结果。VERSION为本文件记录身份，必须唯一；字母序列仅ACGTN（输入可小写），长度等于LOCUS，输出大写。输出所选feature_types按原FEATURES序号定位；其他类型只计ignored，qualifiers保留重复值，不用/translation猜输出序列。
支持位置文法 position := n | n..m | join(position,...) | complement(position)，坐标为1基闭区间，n<=m且在该记录范围。join保持子项写出顺序，包括显式跨圆形末端的join，不按坐标排序；complement对完整子结果取反向互补，因此会逆转片段序并翻转每片方向。A<->T、C<->G、N不变。每个叶区间最终规范为0基半开[start,end)和direction=+/-，按输出拼接顺序列出；相同来源区间重复引用也保留两份。
遇<或>模糊端点、^位点间、order、远程accession位置均把该feature标unsupported并保留原表达式，其他feature继续；语法损坏/越界为invalid feature。来源记录长度/字符/身份错误为整记录拒绝。数据处理不自行补序列、选最大可用区间或推断生物功能。每个输出碱基对应一个原0基位置及方向，mapping可用连续block表达，不要求逐碱基文件；inspect重建任意小片段可核对原碱基与方向。
上限10记录，每记录1000碱基、50 feature、表达式深度5；不做蛋白翻译、复杂基因模型、序列比对或跨记录远程引用。
