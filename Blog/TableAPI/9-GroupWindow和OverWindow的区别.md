# GroupWindow和OverWindow的区别

OverWindow的基础是在于 window上，对window内部进行一定的排序分组等等

GroupWindow就是给数据贴上一个window的标签，然后可以对数据进行操作例如

**区别：**

- groupBy (id,  window)就是先按照 id进行分组，然后在id分组的内部对依照window进行分组。而overWindow一定是先基于window进行分组

- 相对于overWindow来说，groupWindow提供了更多类型的 窗口，例如滑动窗口和滚动窗口等，而overWindow只能设置多少个数据进行一次group操作

  

