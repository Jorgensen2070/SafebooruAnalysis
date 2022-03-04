import React from "react";
import { CircularProgress } from "react-cssfx-loading";
/**
 * This component is used to display a loading circle
 * It can be used to display that data is being fetched
 */
class DatafetchAnimation extends React.Component {
  render() {
    return (
      <div>
        <CircularProgress
          color="#324aa8"
          width="150px"
          height="150px"
          duration="3s"
        />
        <span style={{ color: "#324aa8" }}>Fetching Data</span>
      </div>
    );
  }
}

export default DatafetchAnimation;
