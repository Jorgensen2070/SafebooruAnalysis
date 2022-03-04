import React from "react";
/**
 * This component is used to display a breakLine
 * The cosde is very simple but it was put into this component in order to improve the structure of the other components
 */
class BreakLine extends React.Component {
  render() {
    return (
      <hr
        style={{
          color: "black",
          backgroundColor: "black",
          height: 5,
        }}
      />
    );
  }
}

export default BreakLine;
