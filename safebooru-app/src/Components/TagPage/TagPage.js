import React from "react";
import EChart from "../EChart/EChart";
import resizeObserver from "../EChart/resizeObserver";
import DatafetchAnimation from "../Datafetchanimation/DatafetchAnimation";
import axios from "axios";
import Carousel from "react-bootstrap/Carousel";
import Image from "react-bootstrap/Image";
import { withRouter } from "react-router";
import { Tab, Tabs } from "react-bootstrap";
import BreakLine from "../BreakLine/BreakLine";

/**
 * This Component displays the individual page for the tag
 */
class TagPage extends React.Component {
  constructor(props) {
    super(props);
    this.baseUrl = "http://safeana.f4.htw-berlin.de:8080/";
    //this.baseUrl = "http://localhost:8080/"
    this.fgRef = React.createRef();
    this.parentFigRef = React.createRef();
    this.state = {
      tag: null,
      imageCount: null,
      avgUpScore: null,
      avgUpScoreYearData: null,
      avgUpScoreMonthData: null,
      imageData: null,
      yearChartData: null,
      monthChartData: null,
      dayChartData: null,
      pieChartOption: null,
      tableData: [],
    };
  }

  /**
   * This method obtains the data regarding the imagecounts per year for the tag defined by the given parameter
   * The data is then passed into the options required by echarts and returned
   */
  fetchYearData = async (param) => {
    let yearData = await (
      await axios.get(
        this.baseUrl + "generalTag/year/" + encodeURIComponent(param)
      )
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

    let yearDataDescription = [param];

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
   * This method obtains the data regarding the imagecounts per month for the tag defined by the given parameter
   * The data is then passed into the options required by echarts and returned
   */
  fetchMonthData = async (param) => {
    let monthChartData = await (
      await axios.get(
        this.baseUrl + "generalTag/month/" + encodeURIComponent(param)
      )
    ).data;

    let monthSeriesData = {
      name: param,
      type: "line",
      data: Object.entries(
        Object.entries(Object.entries(monthChartData._3)[0][1])[0][1]
      ).map(([key, value]) => [Object.keys(value)[0], Object.values(value)[0]]),
    };

    let monthDataDescription = [param];

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
   * This method obtains the data regarding the imagecounts per day for the tag defined by the given parameter in the last 31 days
   * The data is then passed into the options required by echarts and returned
   */
  fetchDayData = async (param) => {
    let dayChartData = await (
      await axios.get(
        this.baseUrl + "generalTag/day/" + encodeURIComponent(param)
      )
    ).data;

    let daySeriesData = {
      name: param,
      type: "line",
      data: Object.entries(
        Object.entries(Object.entries(dayChartData._3)[0][1])[0][1]
      ).map(([key, value]) => [Object.keys(value)[0], Object.values(value)[0]]),
    };

    let dayDataDescription = [param];

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
  };

  /**
   * This method obtains up to 5 urls showing the latest images uploaded for the given tag
   * The urls are returned as an array
   */
  fetchLatestImages = async (param) => {
    let imageData = await (
      await axios.get(
        this.baseUrl + "latestImages/" + encodeURIComponent(param)
      )
    ).data;

    return Object.values(Object.entries(imageData)[0][1]).flat();
  };

  /**
   * This method obtains and returns the total average score given images containing the given tag
   */
  fetchAvgUpScore = async (param) => {
    let total = await (
      await axios.get(
        this.baseUrl +
          "generalInformation/avgUpScore/total/" +
          encodeURIComponent(param)
      )
    ).data;

    return Object.values(total[0])[0];
  };

  /**
   * This method obtains the data regarding the average scores per year that were given to images containing the given tag
   * The data is passed into the options required by echarts and returned
   */
  fetchAvgUpScoreYear = async (param) => {
    let yearData = await (
      await axios.get(
        this.baseUrl +
          "generalInformation/avgUpScore/year/" +
          encodeURIComponent(param)
      )
    ).data;

    console.log(yearData);

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

    let yearDataDescription = [param];

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
        text: "Average Score per year",
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
   * This method obtains the data regarding the average scores per month that were given to images containing the given tag
   * The data is passed into the options required by echarts and returned
   */
  fetchAvgUpScoreMonth = async (param) => {
    let monthChartData = await (
      await axios.get(
        this.baseUrl +
          "generalInformation/avgUpScore/month/" +
          encodeURIComponent(param)
      )
    ).data;

    let monthSeriesData = {
      name: param,
      type: "line",
      data: Object.entries(
        Object.entries(Object.entries(monthChartData._3)[0][1])[0][1]
      ).map(([key, value]) => [Object.keys(value)[0], Object.values(value)[0]]),
    };

    console.log(monthSeriesData);

    let monthDataDescription = [param];

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
        text: "Average Score per Month",
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
   * This method obtains and returns the amount of images uploaded for the given tag
   */
  fetchImageCount = async (param) => {
    let total = await (
      await axios.get(
        this.baseUrl + "generalTag/total/" + encodeURIComponent(param)
      )
    ).data;

    return Object.values(total[0])[0];
  };

  /**
   * This method is called when a component is initially rendered here all the data is obtained
   */
  componentDidMount = async () => {
    let param = (this.props.match.params.tag + this.props.location.search)
      .split(" ")
      .join("")
      .replace("%2F", "/");

    try {
      await axios.get(this.baseUrl + "endCurrentJobs");
      this.props.refetchRoutes();

      if (this.state.tag !== param) {
        await axios.get(this.baseUrl + "endCurrentJobs");
        this.props.refetchRoutes();

        this.setState({
          tag: param,
          imageData: null,
          imageCount: null,
          avgUpScore: null,
          avgUpScoreYearData: null,
          avgUpScoreMonthData: null,
          yearChartData: null,
          monthChartData: null,
          dayChartData: null,
        });
      }

      let [year, imageData, count, avgUpScore, avgUpScoreYearData] =
        await Promise.all([
          this.fetchYearData(param),
          this.fetchLatestImages(param),
          this.fetchImageCount(param),
          this.fetchAvgUpScore(param),
          this.fetchAvgUpScoreYear(param),
        ]);
      this.setState({
        tag: param,
        yearChartData: year,
        imageData: imageData,
        imageCount: count,
        avgUpScore: avgUpScore,
        avgUpScoreYearData: avgUpScoreYearData,
      });

      let [month, day, avgUpScoreMonthData] = await Promise.all([
        this.fetchMonthData(param),
        this.fetchDayData(param),
        this.fetchAvgUpScoreMonth(param),
      ]);

      this.setState({
        monthChartData: month,
        dayChartData: day,
        avgUpScoreMonthData: avgUpScoreMonthData,
      });
    } catch (error) {}
  };

  /**
   * This method is called when the component is updated/a change takes place
   * Whenever this happens a check is executred to see if the tag parameter changed
   * If that is the case the site for a new tag was called and thus the new information should be obtained
   * This is done by calling componentDidMount where all the data is obtained
   */
  componentDidUpdate = async () => {
    if (
      this.state.tag !==
      (this.props.match.params.tag + this.props.location.search)
        .split(" ")
        .join("")
        .replace("%2F", "/")
    ) {
      this.componentDidMount();
    }
  };

  render() {
    let tagList = this.props.tagList;
    let type = (this.props.match.params.tag + this.props.location.search)
      .split(" ")
      .join("")
      .replace("%2F", "/");

    console.log("Received tyoe: " + type);

    if (!tagList.includes(type)) {
      this.props.history.push("/error");
    }

    return (
      <div>
        <h1>Tag - {type}</h1>

        <BreakLine />

        <h3>General Information:</h3>

        <div className="d-flex justify-content-center">
          {this.state.imageCount ? (
            <div>
              <h3>This tag has {this.state.imageCount} images in total.</h3>

              <br />

              {this.state.avgUpScore ? (
                <h5>
                  The average Score for this Tag is {this.state.avgUpScore}.
                </h5>
              ) : null}
            </div>
          ) : (
            <DatafetchAnimation />
          )}
        </div>

        <br />

        {this.state.imageData ? (
          <div className="d-flex justify-content-center">
            <div>
              <h1 className="d-flex justify-content-center">Newest images</h1>
              <Carousel
                style={{ width: "1000px", height: "500px" }}
                variant="dark"
              >
                {this.state.imageData.map((entry) => {
                  return (
                    <Carousel.Item>
                      <Image
                        className="d-block mx-auto  my-auto"
                        style={{ height: "500px" }}
                        thumbnail
                        src={entry}
                      />
                    </Carousel.Item>
                  );
                })}
              </Carousel>
            </div>
          </div>
        ) : null}

        <BreakLine />

        <div>
          <h3>Image Count per Year/Month/Day (of the last month)</h3>
          <p>
            Here you can see a graph displaying the amount of images uploaded in
            the given year/month/day for given tag on safebooru.
            <br />
            If no image was uploaded for the given tag in the given
            year/month/day a 0 is displayed.
            <br />
            In order to change between the different tables, please click on the
            Tab-Selector at the top of the graphic
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

        <BreakLine />

        <div>
          <h3>Average score per Year and Month </h3>
          <p>
            Here you can see a graph displaying the evolution of the average
            score of all images of the given tag.
            <br />
            If no score was given for the given tag in the given year/month the
            default value of 0.0 is used.
            <br />
            In order to change between the different tables, please click on the
            Tab-Selector at the top of the graphic
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
              {this.state.avgUpScoreYearData ? (
                <EChart
                  options={this.state.avgUpScoreYearData}
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
              {this.state.avgUpScoreMonthData ? (
                <EChart
                  options={this.state.avgUpScoreMonthData}
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

export default withRouter(TagPage);
