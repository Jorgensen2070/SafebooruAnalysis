import React from "react";

import EChart from "../EChart/EChart";
import resizeObserver from "../EChart/resizeObserver";
import DatafetchAnimation from "../Datafetchanimation/DatafetchAnimation";
import axios from "axios";
import { Tab, Tabs } from "react-bootstrap";
import BreakLine from "../BreakLine/BreakLine";

/**
 * This Component displays the main page for the generalTags
 */
class TagMainPage extends React.Component {
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
      tagBoxplotChartOption: null,
      tableData: [],
    };
  }

  /**
   * This method obtains the data regarding the imagecounts per year for top 9 generalTags
   * The data is then passed into the options required by echarts and returned
   */
  fetchYearData = async () => {
    let yearData = await (
      await axios.get(this.baseUrl + "generalTag/year")
    ).data;

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
   * This method obtains the data regarding the imagecounts per month for top 9 generalTags
   * The data is then passed into the options required by echarts and returned
   */
  fetchMonthData = async () => {
    let monthChartData = await (
      await axios.get(this.baseUrl + "generalTag/month")
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
   * This method obtains the data regarding the imagecounts per day for top 9 generalTags in the last 31 days
   * The data is then passed into the options required by echarts and returned
   */
  fetchDayData = async () => {
    let dayChartData = await (
      await axios.get(this.baseUrl + "generalTag/day")
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
   * This method obtains the data regarding a piechart to show the constellation of the generalTags tags
   * The top 9 entries are detailed
   * The rest is automatically grouped together
   */
  fetchPieData = async () => {
    let copyrightPieChart = await (
      await axios.get(this.baseUrl + "generalTag/total")
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
   * This method obtains the data regarding a boxplot to show the size of the tags
   * The data is then passed into the options required by echarts and returned
   */
  fetchTagBoxplotData = async () => {
    let tagBoxPlot = await (
      await axios.get(this.baseUrl + "generalTag/totalBoxplot")
    ).data;

    let tagBoxplotData = {
      name: "boxplot",
      type: "boxplot",
      data: [tagBoxPlot],
      tooltip: {
        formatter: function (param) {
          return [
            "Tag Boxplot: ",
            "Maximum: " + param.data[5],
            "Q3: " + param.data[4],
            "Median: " + param.data[3],
            "Q1: " + param.data[2],
            "Minimum: " + param.data[1],
          ].join("<br/>");
        },
      },
    };

    let tagBoxplotChartOption = {
      grid: {
        right: "15%",
      },
      title: {
        text: "Image Count per Tag as a Boxplot",
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
        data: ["Tag"],
      },
      series: tagBoxplotData,
    };

    return tagBoxplotChartOption;
  };

  /**
   * This method is called when a component is initially rendered here all the data is obtained
   */
  componentDidMount = async () => {
    try {
      await axios.get(this.baseUrl + "endCurrentJobs");
      this.props.refetchRoutes();

      let [year, pie, tagBox] = await Promise.all([
        this.fetchYearData(),
        this.fetchPieData(),
        this.fetchTagBoxplotData(),
      ]);
      this.setState({
        yearChartData: year,
        pieChartOption: pie,
        tagBoxplotChartOption: tagBox,
      });

      let [month, day] = await Promise.all([
        this.fetchMonthData(),
        this.fetchDayData(),
      ]);

      this.setState({ monthChartData: month, dayChartData: day });
    } catch (error) {}
  };

  render() {
    return (
      <div>
        <div>
          <h1>General-Tags</h1>

          <p>
            The general-tag is a tag which is used to describe information
            present in the image. One example would be the tag "hat" which would
            describe that a hat is present in the given image. An image can have
            many general-tags that are all describing different things found in
            a single image.
            <br />
            <br />
            This page is the main page for the general tags, where you can find
            some Information regarding the top general tags. If you would like
            to see more information for a specific general tag, you can search
            for it in the Searchbar under Tags in the navigation on the left. In
            the navigation bar is also a special site named "Clothing". On this
            site you can find information regarding the composition of the
            different tag groups associated with clothing and the occurrences of
            the different tags within those tag groups.
            <br />
            Please note that during the very first fetch of the day, the loading
            times can be rather long. This is caused by the data not yet being
            in the cache. Subsequent fetches should be considerably faster.
          </p>

          {this.props.tagList ? (
            <h4>
              In total there are {this.props.tagList.length} different general
              tags.
            </h4>
          ) : null}

          <BreakLine />

          <div>
            <h3>Imagecount per Year/Month/Day (of the last month)</h3>
            <p>
              Here you can see a graph displaying the amount of images uploaded
              in the given year/month/day for the 9 most popular tags on
              safebooru.
              <br />
              If no image was uploaded for the given tag in the given
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
          <h3>Total Image Count per General-Tag</h3>
          <p>
            Here you can see a piechart that shows the total image count per tag
            in relation to the overall image count. The tags with the 19 largest
            image counts are detailed and the rest is grouped automatically. As
            you can see, the top 19 tags account for roughly around 50% of all
            tags.
            <br />
            In the second Tab, you can see a Boxplot regarding the image count
            per tag. To see the values for [Min, Q1, Median, Q3, Max] please
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
              {this.state.tagBoxplotChartOption ? (
                <EChart
                  options={this.state.tagBoxplotChartOption}
                  resizeObserver={resizeObserver}
                />
              ) : (
                <DatafetchAnimation />
              )}
            </div>
          </Tab>
        </Tabs>
      </div>
    );
  }
}

export default TagMainPage;
