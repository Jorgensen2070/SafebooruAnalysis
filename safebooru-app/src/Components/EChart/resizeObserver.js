//This returns a ResizeObserver
//This observer recognizes every resize event of the element that it is registered to
//Then we receive the target which is being resized and resize the chart that is located in the resized element

import * as echarts from "echarts";
export default new ResizeObserver((entries) => {
  entries.map(({ target }) => {
    const instance = echarts.getInstanceByDom(target);
    if (instance) {
      instance.resize();
    }
  });
});
