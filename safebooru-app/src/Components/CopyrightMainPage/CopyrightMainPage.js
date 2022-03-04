import React from "react";

import EChart from "../EChart/EChart";
import resizeObserver from "../EChart/resizeObserver";
import DatafetchAnimation from "../Datafetchanimation/DatafetchAnimation";
import axios from "axios";
import { Tab, Tabs, Accordion, Table } from "react-bootstrap";
import { Link } from "react-router-dom";
import { ForceGraph3D } from "react-force-graph";
import SpriteText from "three-spritetext";
import BreakLine from "../BreakLine/BreakLine";

/**
 * This Component displays the main page for the copyrights
 */
class CopyrightMainPage extends React.Component {
  constructor(props) {
    super(props);
    this.baseUrl = "http://safeana.f4.htw-berlin.de:8080/";
    //this.baseUrl = "http://localhost:8080/"

    this.fgRef = React.createRef();
    this.parentFigRef = React.createRef();
    this.state = {
      yearChartData: null,
      monthChartData: null,
      dayChartData: null,
      pieChartOption: null,
      copyBarChartOption: null,
      nodes: null,
      links: null,
      tableData: [],
      tableRendered: false,
    };
  }

  /**
   * This method obtains the data regarding the imagecounts per year for top 9 copyrights (excluding implications)
   * The data is then passed into the options required by echarts and returned
   */
  fetchYearData = async () => {
    let yearData = await (
      await axios.get(this.baseUrl + "copyright/year")
    ).data;
    //return yearData;
    // this.setState({ yearChartData: yearChartOption });
    let yearSeriesData = Object.entries(yearData._3)
      .map((entry) =>
        Object.entries(Object.entries(entry)[1][1]).map(([key, value]) => ({
          name: key,
          type: "line",
          data: value.map((e) => [
            Object.values(e)[0].toString(),
            Object.values(e)[1],
          ]),
        }))
      )
      .flat();

    let yearDataDescription = Object.entries(yearData._3)
      .map((entry) => Object.keys(Object.entries(entry)[1][1]))
      .flat();

    let yearChartOption = {
      legend: {
        data: yearDataDescription,
        type: "scroll",
        orient: "vertical",
        right: 10,
        top: 20,
        bottom: 20,
      },
      grid: {
        right: "20%",
      },
      title: {
        text: "Images per year",
        left: "1%",
      },
      tooltip: {
        trigger: "axis",
      },
      xAxis: {
        type: "time",
        axisLabel: {
          formatter: (val) => {
            var date = new Date(val).toLocaleString();
            return date.slice(4, 8);
          },
        },
        min: yearData._1.toString(),
        max: yearData._2.toString(),
        boundaryGap: false,
      },
      yAxis: {},
      series: yearSeriesData,
    };
    return yearChartOption;
  };

  /**
   * This method obtains the data regarding the imagecounts per month for top 9 copyrights (excluding implications)
   * The data is then passed into the options required by echarts and returned
   */
  fetchMonthData = async () => {
    let monthChartData = await (
      await axios.get(this.baseUrl + "copyright/month")
    ).data;

    let monthSeriesData = Object.entries(monthChartData._3)
      .map((entry) =>
        Object.entries(Object.entries(entry)[1][1]).map(([key, value]) => ({
          name: key,
          type: "line",
          data: value.map((e) => [Object.keys(e)[0], Object.values(e)[0]]),
        }))
      )
      .flat();

    let monthDataDescription = Object.entries(monthChartData._3)
      .map((entry) => Object.keys(Object.entries(entry)[1][1]))
      .flat();

    let monthChartOption = {
      large: true,
      legend: {
        data: monthDataDescription,
        type: "scroll",
        orient: "vertical",
        right: 10,
        top: 20,
        bottom: 20,
      },
      grid: {
        right: "20%",
      },
      title: {
        text: "Images per Month",
        left: "1%",
      },
      tooltip: {
        trigger: "axis",
      },
      dataZoom: [
        {
          type: "inside",
          start: 50,
          end: 100,
        },
        {
          show: true,
          type: "slider",
          top: "90%",
          start: 50,
          end: 100,
        },
      ],
      xAxis: {
        type: "time",
        axisLabel: {
          formatter: (val) => {
            var date = new Date(val).toLocaleString();
            return date.slice(2, 8);
          },
        },
        min: monthChartData._1.toString(),
        max: monthChartData._2.toString(),
        boundaryGap: false,
      },
      yAxis: {},
      series: monthSeriesData,
    };
    return monthChartOption;
  };

  /**
   * This method obtains the data regarding the imagecounts per day for top 9 copyrights (excluding implications) in the last 31 days
   * The data is then passed into the options required by echarts and returned
   */
  fetchDayData = async () => {
    let dayChartData = await (
      await axios.get(this.baseUrl + "copyright/day")
    ).data;

    let daySeriesData = Object.entries(dayChartData._3)
      .map((entry) =>
        Object.entries(Object.entries(entry)[1][1]).map(([key, value]) => ({
          name: key,
          type: "line",
          data: value.map((e) => [Object.keys(e)[0], Object.values(e)[0]]),
        }))
      )
      .flat();

    let dayDataDescription = Object.entries(dayChartData._3)
      .map((entry) => Object.keys(Object.entries(entry)[1][1]))
      .flat();

    let dayChartOption = {
      large: true,
      legend: {
        data: dayDataDescription,
        type: "scroll",
        orient: "vertical",
        right: 10,
        top: 20,
        bottom: 20,
      },
      grid: {
        right: "20%",
      },
      title: {
        text: "Images per Day",
        left: "1%",
      },
      tooltip: {
        trigger: "axis",
      },
      dataZoom: [
        {
          type: "inside",
          start: 50,
          end: 100,
        },
        {
          show: true,
          type: "slider",
          top: "90%",
          start: 50,
          end: 100,
        },
      ],
      xAxis: {
        type: "time",
        axisLabel: {
          formatter: (val) => {
            var date = new Date(val).toLocaleString();
            return date.slice(0, 10);
          },
        },
        min: dayChartData._1.toString(),
        max: dayChartData._2.toString(),
        boundaryGap: false,
      },
      yAxis: {},
      series: daySeriesData,
    };
    return dayChartOption;
    //this.setState({ dayChartData: dayChartOption });
  };

  /**
   * This method obtains the data regarding a piechart to show the constellation of the copyright tags
   * The entries that  contain more than 30.000 entries are detailed
   * The rest is automatically grouped together
   */
  fetchPieData = async () => {
    let copyrightPieChart = await (
      await axios.get(this.baseUrl + "copyright/total")
    ).data;

    let copyrightPieData = {
      type: "pie",
      name: "Copyright Data",
      data: copyrightPieChart
        .map((entry) => [
          {
            value: Object.values(entry)[0],
            name: Object.keys(entry).toString(),
          },
        ])
        .flat(),
    };

    let pieChartOption = {
      grid: {
        right: "15%",
      },
      title: {
        text: "Total Image Count",
        left: "1%",
      },
      tooltip: {
        trigger: "item",
      },
      legend: {
        orient: "vertical",
        type: "scroll",
        right: 10,
        top: 20,
        bottom: 20,
      },
      series: copyrightPieData,
    };

    return pieChartOption;
  };

  /**
   * This method obtains the data regarding a boxplot to show the size of the copyright tags
   * The data is then passed into the options required by echarts and returned
   */
  fetchCopyBoxplotData = async () => {
    let copyrightBoxPlot = await (
      await axios.get(this.baseUrl + "copyright/totalBoxplot")
    ).data;

    let copyrightBoxplotData = {
      name: "boxplot",
      type: "boxplot",
      data: [copyrightBoxPlot],
      tooltip: {
        formatter: function (param) {
          return [
            "Copyright Boxplot: ",
            "Maximum: " + param.data[5],
            "Q3: " + param.data[4],
            "Median: " + param.data[3],
            "Q1: " + param.data[2],
            "Minimum: " + param.data[1],
          ].join("<br/>");
        },
      },
    };

    let copyBoxplotChartOption = {
      grid: {
        right: "15%",
      },
      title: {
        text: "Image Count per Copyright as a Boxplot",
        left: "1%",
      },
      tooltip: {
        trigger: "item",
        confine: true,
      },
      xAxis: {
        name: "Count",
        nameLocation: "start",
        scale: true,
      },
      yAxis: {
        type: "category",
        data: ["Copyright"],
      },
      series: copyrightBoxplotData,
    };

    return copyBoxplotChartOption;
  };

  /**
   * This method obtains the data regarding the connections between the top 100 copyrights and the nodes of the graph
   * 2 values are returned
   * 1 - the nodes with the following values (id,name,count,group)
   * 2 - the links in the graph with the following values (source,taget,distance)
   */
  fetchCopyConnectionData = async () => {
    let data = await (
      await axios.get(this.baseUrl + "copyright/copyrightPairing")
    ).data;

    let nodes = data._1.map((elem) => ({
      id: elem[Object.keys(elem)[0]],
      name: elem[Object.keys(elem)[0]].toString().replaceAll("_", " "),
      val: elem[Object.keys(elem)[1]],
      group: elem[Object.keys(elem)[2]],
    }));

    let links = data._2.map((elem) => ({
      source: elem[Object.keys(elem)[0]],
      target: elem[Object.keys(elem)[1]],
      distance: elem[Object.keys(elem)[2]],
    }));

    return [nodes, links];
  };

  /**
   * This method is called when a component is initially rendered here all the data is obtained
   */
  componentDidMount = async () => {
    try {
      await axios.get(this.baseUrl + "endCurrentJobs");
      this.props.refetchRoutes();

      let [year, pie, copyBox] = await Promise.all([
        this.fetchYearData(),
        this.fetchPieData(),
        this.fetchCopyBoxplotData(),
      ]);
      this.setState({
        yearChartData: year,
        pieChartOption: pie,
        copyBoxplotChartOption: copyBox,
      });

      let [month, day, forceData] = await Promise.all([
        this.fetchMonthData(),
        this.fetchDayData(),
        this.fetchCopyConnectionData(),
      ]);

      this.setState(
        { monthChartData: month, dayChartData: day, nodes: forceData[0] },
        () =>
          this.setState({
            links: forceData[1],
            maxCharCount: forceData[0][0].val,
            maxConnectionCount: forceData[1][0].distance,
          })
      );

      if (this.fgRef.current) {
        this.fgRef.current
          .d3Force("link")
          .distance(
            (link) =>
              Math.pow(link.distance, -1) * (this.state.maxConnectionCount * 5)
          );
      }

      if (this.state.tableRendered !== true) {
        let temp = forceData[1].map((entry) => [
          entry.source,
          entry.target,
          entry.distance,
        ]);

        this.setState({ tableData: temp, tableRendered: true });
      }
    } catch (error) {}
  };

  /**
   * This method moves the cameran in a force directed graph to the selected node
   * @param {*} node - the node that was selected
   */
  nodeClicked = (node) => {
    const distance = 40;
    const distRatio = 1 + distance / Math.hypot(node.x, node.y, node.z);

    this.fgRef.current.cameraPosition(
      { x: node.x * distRatio, y: node.y * distRatio, z: node.z * distRatio }, // new position
      node, // lookAt ({ x, y, z })
      3000 // ms transition duration
    );
  };

  render() {
    return (
      <div>
        <div>
          <h1>Copyright</h1>

          <p>
            The copyright is a tag which indicates the series or domain the
            image or a character shown in the image originates from. An image
            can have multiple copyrights if for example 2 characters from
            different series are drawn together or the given character is
            present in multiple copyrights.
            <br />
            In addition to normal copyrights, there are also related copyrights.
            Related copyrights are copyrights where one copyrights originates
            from another one. One example for this is the "touhou_(pc-98)"
            copyright, which originates from the "touhou" copyright and in this
            case is a game within the Touhou franchise.
            <br />
            <br />
            This page is the main page for the copyrights, where you can find
            some Information regarding the top Copyrights. If you would like to
            see more information for a specific copyright, you can search for it
            in the Searchbar under Copyright in the navigation on the left or
            click on the links of the top copyrights.
            <br />
            Please note that during the very first fetch of the day, the loading
            times can be rather long. This is caused by the data not yet being
            in the cache. Subsequent fetches should be considerably faster.
          </p>

          {this.props.copyList ? (
            <h4>In total there are {this.props.copyList.length} copyrights.</h4>
          ) : null}

          <BreakLine />

          <div>
            <h3>Image Count per Year/Month/Day (of the last month)</h3>
            <p>
              Here you can see a graph displaying the amount of images uploaded
              in the given year/month/day for the 9 most popular copyrights on
              safebooru.
              <br />
              If no image was uploaded for the given copyright in the given
              year/month/day a 0 is displayed.
              <br />
              In order to change between the different tables, please click on
              the Tab-Selector at the top of the graphic
            </p>
          </div>

          <Tabs
            defaultActiveKey="years"
            id="uncontrolled-tab-example"
            className="mb-3"
          >
            <Tab eventKey="years" title="Year">
              <div
                className="d-flex justify-content-center align-items-center"
                style={{ minWidth: "100%", height: 500 }}
              >
                {this.state.yearChartData ? (
                  <EChart
                    options={this.state.yearChartData}
                    resizeObserver={resizeObserver}
                  />
                ) : (
                  <DatafetchAnimation />
                )}
              </div>
            </Tab>
            <Tab eventKey="months" title="Month" className="mb-3">
              <div
                className="d-flex justify-content-center align-items-center"
                style={{
                  minWidth: "100%",
                  height: 500,
                }}
              >
                {this.state.monthChartData ? (
                  <EChart
                    options={this.state.monthChartData}
                    resizeObserver={resizeObserver}
                  />
                ) : (
                  <DatafetchAnimation />
                )}
              </div>
            </Tab>
            <Tab eventKey="days" title="Last Month" className="mb-3">
              <div
                className="d-flex justify-content-center align-items-center"
                style={{ minWidth: "100%", height: 500 }}
              >
                {this.state.dayChartData ? (
                  <EChart
                    options={this.state.dayChartData}
                    resizeObserver={resizeObserver}
                  />
                ) : (
                  <DatafetchAnimation />
                )}
              </div>
            </Tab>
          </Tabs>
        </div>

        <BreakLine />

        <div>
          <h3>Total Image Count per Copyright</h3>
          <p>
            Here you can see a piechart that shows the total image count per
            Copyright in relation to the overall image count. The copyrights
            which contain more than 30.000 images are detailed and the rest is
            grouped automatically. As you can see the top 19 Copyrights account
            for more than half of the images in Safebooru.
            <br />
            In the second Tab, you can see a Boxplot regarding the image count
            per Copyright. To see the values for [Min,Q1,Median,Q3,Max] please
            hover over it with the mouse.
          </p>
        </div>

        <Tabs
          defaultActiveKey="pie"
          id="uncontrolled-tab-example"
          className="mb-3"
        >
          <Tab eventKey="pie" title="Piechart" className="mb-3">
            <div
              className="d-flex justify-content-center align-items-center"
              style={{ width: "100%", height: 500 }}
            >
              {this.state.pieChartOption ? (
                <EChart
                  options={this.state.pieChartOption}
                  resizeObserver={resizeObserver}
                />
              ) : (
                <DatafetchAnimation />
              )}
            </div>
          </Tab>

          <Tab eventKey="boxplot" title="Boxplot" className="mb-3">
            <div
              className="d-flex justify-content-center align-items-center"
              style={{ width: "100%", height: 500 }}
            >
              {this.state.copyBoxplotChartOption ? (
                <EChart
                  options={this.state.copyBoxplotChartOption}
                  resizeObserver={resizeObserver}
                />
              ) : (
                <DatafetchAnimation />
              )}
            </div>
          </Tab>
        </Tabs>

        <BreakLine />
        <div>
          <h3>Force-directed graph of copyright combinations</h3>
          <p>
            Here you can see a force-directed graph displaying the connections
            between the 100 largest copyrights on Safebooru. A connection
            between two copyrights is generated when an image was tagged with
            both those copyrights.
          </p>
          <p>
            The width of the line between two copyrights indicates how many
            connections there are.
            <br />
            The number behind the name of a node indicates how many total images
            with the given character exist.
            <br />
            The closer a copyright is to the center, the more often it was
            tagged together with others.
            <br />
            The farther a copyright is away from the centre, the less often it
            was tagged together with other copyrights.
          </p>
          <p>
            In some cases the initial force is not applied e.g., if the window
            is not open when the site is rerendered with the data regarding the
            links. To solve this issue, please click inside the window of the
            force graph and then the force should be reapplied.
            <br />
            You can then navigate through the network either by using your mouse
            or by clicking on the nodes.
          </p>
        </div>

        <div
          className="d-flex justify-content-center align-items-center"
          style={{ width: "100%", height: 500 }}
          ref={this.parentFigRef}
        >
          {this.state.links ? (
            <div>
              <ForceGraph3D
                warmupTicks={100}
                cooldownTicks={0}
                ref={this.fgRef}
                height={500}
                width={this.parentFigRef.current.offsetWidth}
                //nodeAutoColorBy="group"
                nodeColor="white"
                nodeThreeObject={(node) => {
                  const sprite = new SpriteText(
                    node.name
                      .split(" ")
                      .map(
                        (word) => word.charAt(0).toUpperCase() + word.slice(1)
                      )
                      .join(" ") +
                      " (" +
                      node.val +
                      ")"
                  );
                  sprite.color = "grey";
                  sprite.textHeight = 500;
                  return sprite;
                }}
                nodeThreeObjectExtend={true}
                onNodeClick={(node) => this.nodeClicked(node)}
                linkWidth={(link) =>
                  (Math.pow(
                    link.distance / this.state.maxConnectionCount,
                    0.25
                  ) *
                    this.state.maxConnectionCount) /
                  500
                }
                linkColor={(link) =>
                  "rgba(255, 255, 255, " +
                  Math.pow(
                    link.distance / this.state.maxConnectionCount,
                    0.25
                  ) +
                  ")"
                }
                graphData={{ nodes: this.state.nodes, links: this.state.links }}
              />

              <br></br>

              <Accordion>
                <Accordion.Item eventKey="0">
                  <Accordion.Header>
                    Copyright Connection Table
                  </Accordion.Header>
                  <Accordion.Body>
                    <div
                      className="table-responsive"
                      style={{ height: "250px" }}
                    >
                      <Table>
                        <thead>
                          <tr>
                            <th>Copyright-1</th>
                            <th>Copyright-2</th>
                            <th>Count</th>
                          </tr>
                        </thead>
                        <tbody>
                          {this.state.tableData.map((entry, index) => {
                            return (
                              <tr key={index}>
                                <td>
                                  <Link
                                    style={{
                                      color: "purple",
                                      fontSize: "20px",
                                    }}
                                    to={"/copyright/" + entry[0]}
                                  >
                                    {entry[0]}
                                  </Link>
                                </td>
                                <td>
                                  <Link
                                    style={{
                                      color: "purple",
                                      fontSize: "20px",
                                    }}
                                    to={"/copyright/" + entry[1]}
                                  >
                                    {entry[1]}
                                  </Link>
                                </td>
                                <td>{entry[2]}</td>
                              </tr>
                            );
                          })}
                        </tbody>
                      </Table>
                    </div>
                  </Accordion.Body>
                </Accordion.Item>{" "}
              </Accordion>
            </div>
          ) : (
            <DatafetchAnimation />
          )}
        </div>
      </div>
    );
  }
}

export default CopyrightMainPage;
