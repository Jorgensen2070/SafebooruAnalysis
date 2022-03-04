import React, { useRef, useEffect } from "react";
import PropTypes from "prop-types";
import * as echarts from "echarts";

/**
 * This function returns a component containing an echart
 *
 * @param {*} param0 - {options for the echart graphic,resizeObverser responsible for resizing the chart}
 * @returns the generated echart
 */
function EChart({ options, resizeObserver }) {
  const myChart = useRef(null);
  useEffect(() => {
    const chart = echarts.init(myChart.current);
    chart.setOption(options);
    if (resizeObserver) resizeObserver.observe(myChart.current);
  }, [options, resizeObserver]);

  return (
    <div
      ref={myChart}
      style={{
        width: "100%",
        height: "100%",
      }}
    ></div>
  );
}

EChart.propTypes = {
  options: PropTypes.any,
  resizeObserver: PropTypes.oneOfType([PropTypes.func, PropTypes.object]),
};

export default EChart;
