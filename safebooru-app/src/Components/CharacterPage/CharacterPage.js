import React from "react";
import EChart from "../EChart/EChart";
import resizeObserver from "../EChart/resizeObserver";
import axios from "axios";
import DatafetchAnimation from "../Datafetchanimation/DatafetchAnimation";
import Carousel from "react-bootstrap/Carousel";
import Image from "react-bootstrap/Image";
import { Link } from "react-router-dom";
import { withRouter } from "react-router";
import { Tab, Tabs, Accordion, Table } from "react-bootstrap";
import BreakLine from "../BreakLine/BreakLine";

/**
 * This Component displays the individual page for the characters
 */
class CharacterPage extends React.Component {
  constructor(props) {
    super(props);
    this.baseUrl = "http://safeana.f4.htw-berlin.de:8080/";
    //this.baseUrl = "http://localhost:8080/"
    this.fgRef = React.createRef();
    this.parentFigRef = React.createRef();
    this.state = {
      character: null,
      imageCount: null,
      avgUpScore: null,
      avgUpScoreYearData: null,
      avgUpScoreMonthData: null,
      mainCopyright: null,
      imageData: null,
      yearChartData: null,
      monthChartData: null,
      dayChartData: null,
      tagRadarChartData: null,
      notEnoughDataForRadar: false,
      tagTableData: [],
      characterPairingPieChartData: null,
      characterPairingTableData: [],
      copyrightPairingTableData: [],
      copyrightPairingPieChartData: null,
    };
  }

  /**
   * This method obtains the data regarding the imagecounts per year for the character defined by the given parameter
   * The data is then passed into the options required by echarts and returned
   */
  fetchYearData = async (param) => {
    let yearData = await (
      await axios.get(
        this.baseUrl + "character/year/" + encodeURIComponent(param)
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
   * This method obtains the data regarding the imagecounts per month for the character defined by the given parameter
   * The data is then passed into the options required by echarts and returned
   */
  fetchMonthData = async (param) => {
    let monthChartData = await (
      await axios.get(
        this.baseUrl + "character/month/" + encodeURIComponent(param)
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
   * This method obtains the data regarding the imagecounts per day for the character defined by the given parameter in the last 31 days
   * The data is then passed into the options required by echarts and returned
   */
  fetchDayData = async (param) => {
    let dayChartData = await (
      await axios.get(
        this.baseUrl + "character/day/" + encodeURIComponent(param)
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
   * This method obtains up to 5 urls showing the latest images uploaded for the given character
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
   * This method obtains and returns the amount of images uploaded for the given character
   */
  fetchImageCount = async (param) => {
    let total = await (
      await axios.get(
        this.baseUrl + "character/total/" + encodeURIComponent(param)
      )
    ).data;

    return Object.values(total[0])[0];
  };

  /**
   * This method obtains and returns the total average score given images containing the given character
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
   * This method obtains the data regarding the average scores per year that were given to images containing the given character
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
   * This method obtains the data regarding the average scores per month that were given to images containing the given character
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
   * This method obtains and returns the maincopyright of the given charactert
   */
  fetchMainCopyright = async (param) => {
    let total = await (
      await axios.get(
        this.baseUrl + "character/mainCopyright/" + encodeURIComponent(param)
      )
    ).data;

    return total;
  };

  /**
   * This method obtains data regarding the top 10 generalTags that the given character was tagged with
   * The data is then passed into the options required by echarts and returned
   * The options are used to generate a so called radarChart
   */
  fetchTagRadar = async (param) => {
    let radarChartData = await (
      await axios.get(
        this.baseUrl + "character/tagsRadar/" + encodeURIComponent(param)
      )
    ).data;

    if (radarChartData._2.length === 0) {
      return null;
    }

    let indicatorData = radarChartData._2
      .map((val) => ({
        name: val,
        max: radarChartData._1,
      }))
      .flat();

    let radarChartOption = {
      title: {
        text: "Tag Radar Chart",
      },
      legend: {
        data: [param],
      },
      radar: {
        // shape: 'circle',
        indicator: indicatorData,
      },
      tooltip: {
        trigger: "item",
      },
      series: [
        {
          name: param,
          type: "radar",
          data: [
            {
              value: radarChartData._3,
              name: param,
            },
          ],
        },
      ],
    };
    return radarChartOption;
  };

  /**
   * This method obtains and returns a list of generalTags that the given character was tagged with together with a count of occurrences
   * (at least 5 combinations need to be present for the connection to be saved)
   */
  fetchTagList = async (param) => {
    let tagList = await (
      await axios.get(
        this.baseUrl + "character/tags/" + encodeURIComponent(param)
      )
    ).data;

    let tableData = tagList.map((entry) => [
      Object.values(entry)[0],
      Object.keys(entry).toString(),
    ]);

    return tableData;
  };

  /**
   * This method obtains and returns a list of characters that the given character was drawn together with and a count of occurrences
   */
  fetchCharacterPairingList = async (param) => {
    let tagList = await (
      await axios.get(
        this.baseUrl + "character/characterPairing/" + encodeURIComponent(param)
      )
    ).data;

    let tableData = tagList.map((entry) => [
      Object.values(entry)[0],
      Object.keys(entry).toString(),
    ]);

    return tableData;
  };

  /**
   * This method obtains data regarding characters that the given character was drawn together with that is used to create a piechart
   * The first 19 entries are detailed and the rest is automatically grouped
   * The data is then passed into the options required by echarts and returned
   */
  fetchCharacterPairingPie = async (param) => {
    let characterPieChart = await (
      await axios.get(
        this.baseUrl +
          "character/characterPairingPie/" +
          encodeURIComponent(param)
      )
    ).data;

    let characterPieData = {
      type: "pie",
      name: "Character Count",
      data: characterPieChart
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
        text: "Character Connection Count",
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
      series: characterPieData,
    };

    return pieChartOption;
  };

  /**
   * This method obtains and returns a list of copyrights that the given character was tagged together with and a count of occurrences
   */
  fetchCopyrightConnectionList = async (param) => {
    let tagList = await (
      await axios.get(
        this.baseUrl + "character/copyright/" + encodeURIComponent(param)
      )
    ).data;

    let tableData = tagList.map((entry) => [
      Object.values(entry)[0],
      Object.keys(entry).toString(),
    ]);

    return tableData;
  };

  /**
   * This method obtains data regarding copyrights that the given character was tagged together with that is used to create a piechart
   * The first 19 entries are detailed and the rest is automatically grouped
   * The data is then passed into the options required by echarts and returned
   */
  fetchCopyrightConnectionPie = async (param) => {
    let copyrightList = await (
      await axios.get(
        this.baseUrl + "character/copyrightPie/" + encodeURIComponent(param)
      )
    ).data;

    let characterPieData = {
      type: "pie",
      name: "Copyrigth Count",
      data: copyrightList
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
        text: "Copyright Connection Count",
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
      series: characterPieData,
    };

    return pieChartOption;
  };

  /**
   * This method is called wehen the component is mounted or a call in the componentDidUpdate method occurres
   * In this method the data for the charts and tables is obatined and set
   */
  componentDidMount = async () => {
    //Thank you okina baba for giving us a series with the name "kumo desu ga nani ka?"
    //-> the ? requires special treatment
    let param = (this.props.match.params.character + this.props.location.search)
      .split(" ")
      .join("")
      .replace("%2F", "/");

    try {
      if (!this.state.character) {
        await axios.get(this.baseUrl + "endCurrentJobs");
        this.props.refetchRoutes();
      }

      if (this.state.character !== param) {
        await axios.get(this.baseUrl + "endCurrentJobs");
        this.props.refetchRoutes();

        /**
         * Here everything is reset to null in order for the loading animations to reapper
         */
        this.setState({
          character: param,
          imageCount: null,
          avgUpScore: null,
          avgUpScoreYearData: null,
          avgUpScoreMonthData: null,
          mainCopyright: null,
          imageData: null,
          yearChartData: null,
          monthChartData: null,
          dayChartData: null,
          tagRadarChartData: null,
          notEnoughDataForRadar: false,
          tagTableData: [],
          characterPairingTableData: [],
          characterPairingPieChartData: null,
          copyrightPairingTableData: [],
          copyrightPairingPieChartData: null,
        });
      }

      let [
        year,
        imageData,
        imageCount,
        avgUpScore,
        avgUpScoreYearData,
        mainCopyright,
        tagRadar,
        tagList,
        characterPairingList,
        characterPairingPie,
        copyrightPairingList,
        copyrightPairingPie,
      ] = await Promise.all([
        this.fetchYearData(param),
        this.fetchLatestImages(param),
        this.fetchImageCount(param),
        this.fetchAvgUpScore(param),
        this.fetchAvgUpScoreYear(param),
        this.fetchMainCopyright(param),
        this.fetchTagRadar(param),
        this.fetchTagList(param),
        this.fetchCharacterPairingList(param),
        this.fetchCharacterPairingPie(param),
        this.fetchCopyrightConnectionList(param),
        this.fetchCopyrightConnectionPie(param),
      ]);

      this.setState({
        character: param,
        yearChartData: year,
        imageData: imageData,
        imageCount: imageCount,
        avgUpScore: avgUpScore,
        avgUpScoreYearData: avgUpScoreYearData,
        mainCopyright: mainCopyright,
        tagTableData: tagList,
        characterPairingTableData: characterPairingList,
        characterPairingPieChartData: characterPairingPie,
        copyrightPairingTableData: copyrightPairingList,
        copyrightPairingPieChartData: copyrightPairingPie,
      });

      if (tagRadar) {
        this.setState({
          tagRadarChartData: tagRadar,
        });
      } else {
        this.setState({
          notEnoughDataForRadar: true,
        });
      }

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
   * Whenever this happens a check is executred to see if the character parameter changed
   * If that is the case the site for a new character was called and thus the new information should be obtained
   * This is done by calling componentDidMount where all the data is obtained
   */
  componentDidUpdate = async () => {
    if (
      this.state.character !==
      (this.props.match.params.character + this.props.location.search)
        .split(" ")
        .join("")
        .replace("%2F", "/")
    ) {
      this.componentDidMount();
    }
  };

  render() {
    let charList = this.props.charList;
    let type = (this.props.match.params.character + this.props.location.search)
      .split(" ")
      .join("")
      .replace("%2F", "/");

    if (!charList.includes(type)) {
      this.props.history.push("/error");
    }

    return (
      <div>
        <h1>Character - {type}</h1>

        <BreakLine />

        <h3>General Information:</h3>

        <div className="d-flex justify-content-center">
          {this.state.imageCount ? (
            <div>
              <h3>
                This character has {this.state.imageCount} images in total.
              </h3>

              <h5>
                {" "}
                This character originates from the following copyright:{" "}
                <Link
                  style={{ color: "purple", fontSize: "20px" }}
                  to={"/copyright/" + this.state.mainCopyright}
                >
                  {this.state.mainCopyright}
                </Link>{" "}
                (main copyright of the character)
              </h5>
              <br />

              {this.state.characterPairingTableData.length > 0 ? (
                <h5>
                  This character was drawn together with{" "}
                  {this.state.characterPairingTableData.length} other
                  characters(s).
                </h5>
              ) : null}
              <br />
              {this.state.tagTableData.length > 0 ? (
                <h5>
                  Images of this character contain in total{" "}
                  {this.state.tagTableData.length} different tag(s).
                </h5>
              ) : null}
              {this.state.avgUpScore ? (
                <h5>
                  The average Score for this Character is{" "}
                  {this.state.avgUpScore}.
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
            the given year/month/day for the given character.
            <br />
            If no image was uploaded for the given character in the given
            year/month/day a 0 is displayed.
            <br />
            In order to change between the different tables, please click on the
            Tab-Selector at the top of the graphic.
          </p>
        </div>

        <div>
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
          <h3>Tags for the given character</h3>
          <p>
            Here you can see the 10 most common tags given to this character in
            relation to the total image count of the character. If you would
            like to see more information regarding the connections to all tags,
            you can find that information in the table below the graph.
          </p>
        </div>

        <div>
          <div
            className="d-flex justify-content-center align-items-center"
            style={{ minWidth: "100%", height: 500 }}
          >
            {this.state.notEnoughDataForRadar ? (
              <h3>
                There is not enough data regarding connections to tags to
                generate a graph from it. At least 5 connections to a tag are
                required.
              </h3>
            ) : this.state.tagRadarChartData ? (
              <EChart
                options={this.state.tagRadarChartData}
                resizeObserver={resizeObserver}
              />
            ) : (
              <DatafetchAnimation />
            )}
          </div>
          <Accordion>
            <Accordion.Item eventKey="0">
              <Accordion.Header>Tag connection table</Accordion.Header>
              <Accordion.Body>
                <div className="table-responsive" style={{ height: "250px" }}>
                  <Table>
                    <thead>
                      <tr>
                        <th>Count</th>
                        <th>Tag</th>
                      </tr>
                    </thead>
                    <tbody>
                      {this.state.tagTableData.map((entry, index) => {
                        return (
                          <tr key={index}>
                            <td>{entry[0]}</td>
                            <td>
                              <Link
                                style={{
                                  color: "blue",
                                  fontSize: "20px",
                                }}
                                to={"/tag/" + entry[1]}
                              >
                                {entry[1]}
                              </Link>
                            </td>
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

        <BreakLine />

        <div>
          <h3>Connections to other characters</h3>
          <p>
            Here you can see a piechart displaying the connections from this
            character to other characters. The first 19 entries are detailed and
            the rest is grouped together in order to not clutter the graph. You
            can find detailed information regarding the connections in the table
            below the graph.
          </p>
        </div>

        <div>
          <div
            className="d-flex justify-content-center align-items-center"
            style={{ minWidth: "100%", height: 500 }}
          >
            {this.state.characterPairingPieChartData ? (
              <EChart
                options={this.state.characterPairingPieChartData}
                resizeObserver={resizeObserver}
              />
            ) : (
              <DatafetchAnimation />
            )}
          </div>
          <Accordion>
            <Accordion.Item eventKey="0">
              <Accordion.Header>Character connection table</Accordion.Header>
              <Accordion.Body>
                <div className="table-responsive" style={{ height: "250px" }}>
                  <Table>
                    <thead>
                      <tr>
                        <th>Count</th>
                        <th>Tag</th>
                      </tr>
                    </thead>
                    <tbody>
                      {this.state.characterPairingTableData.map(
                        (entry, index) => {
                          return (
                            <tr key={index}>
                              <td>{entry[0]}</td>
                              <td>
                                <Link
                                  style={{
                                    color: "purple",
                                    fontSize: "20px",
                                  }}
                                  to={"/character/" + entry[1]}
                                >
                                  {entry[1]}
                                </Link>
                              </td>
                            </tr>
                          );
                        }
                      )}
                    </tbody>
                  </Table>
                </div>
              </Accordion.Body>
            </Accordion.Item>{" "}
          </Accordion>
        </div>

        <BreakLine />

        <div>
          <h3>Connections to other copyrights</h3>
          <p>
            Here you can see a piechart displaying the connections from this
            character to other copyrights besides their main copyright. The
            first 19 entries are detailed and the rest is grouped together in
            order to not clutter the graph. You can find detailed information
            regarding the connections, including the one to the main copyright
            in the table below the graph.
          </p>
        </div>

        <div>
          <div
            className="d-flex justify-content-center align-items-center"
            style={{ minWidth: "100%", height: 500 }}
          >
            {this.state.copyrightPairingPieChartData ? (
              <EChart
                options={this.state.copyrightPairingPieChartData}
                resizeObserver={resizeObserver}
              />
            ) : (
              <DatafetchAnimation />
            )}
          </div>
          <Accordion>
            <Accordion.Item eventKey="0">
              <Accordion.Header>Copyright Connection Table</Accordion.Header>
              <Accordion.Body>
                <div className="table-responsive" style={{ height: "250px" }}>
                  <Table>
                    <thead>
                      <tr>
                        <th>Count</th>
                        <th>Copyright</th>
                      </tr>
                    </thead>
                    <tbody>
                      {this.state.copyrightPairingTableData.map(
                        (entry, index) => {
                          return (
                            <tr key={index}>
                              <td>{entry[0]}</td>
                              <td>
                                <Link
                                  style={{
                                    color: "purples",
                                    fontSize: "20px",
                                  }}
                                  to={"/copyright/" + entry[1]}
                                >
                                  {entry[1]}
                                </Link>
                              </td>
                            </tr>
                          );
                        }
                      )}
                    </tbody>
                  </Table>
                </div>
              </Accordion.Body>
            </Accordion.Item>{" "}
          </Accordion>
        </div>

        <BreakLine />

        <div>
          <h3>Average score per year and month </h3>
          <p>
            Here you can see a graph displaying the evolution of the average
            score of all images of the given character.
            <br />
            If no score was given for the given character in the given
            year/month the default value of 0.0 is used.
            <br />
            In order to change between the different graphs, please click on the
            Tab-Selector at the top of the graphic.
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

export default withRouter(CharacterPage);
