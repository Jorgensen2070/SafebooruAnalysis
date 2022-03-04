import React from "react";
import EChart from "../EChart/EChart";
import resizeObserver from "../EChart/resizeObserver";
import axios from "axios";
import DatafetchAnimation from "../Datafetchanimation/DatafetchAnimation";
import { ForceGraph3D } from "react-force-graph";
import SpriteText from "three-spritetext";
import Carousel from "react-bootstrap/Carousel";
import Image from "react-bootstrap/Image";
import { Link } from "react-router-dom";
import { withRouter } from "react-router";
import { Tab, Tabs, Accordion, Table } from "react-bootstrap";
import BreakLine from "../BreakLine/BreakLine";

/**
 * This Component displays the individual page for the copyrigths
 */
class CopyrightPage extends React.Component {
  constructor(props) {
    super(props);
    this.baseUrl = "http://safeana.f4.htw-berlin.de:8080/";
    //this.baseUrl = "http://localhost:8080/"

    this.fgRef = React.createRef();
    this.parentFigRef = React.createRef();
    this.state = {
      copyright: null,
      imageCount: null,
      imageData: null,
      yearChartData: null,
      monthChartData: null,
      dayChartData: null,
      avgUpScore: null,
      avgUpScoreYearData: null,
      avgUpScoreMonthData: null,
      copyrightConnectionPieData: null,
      allCharPieData: null,
      mainCharPieData: null,
      topCharactersYearData: null,
      topCharactersMonthData: null,
      topCharactersDayData: null,
      originating: [],
      implications: [],
      nodes: null,
      links: null,
      copyrightConnectionTableData: [],
      charConnectionTableData: [],
      allCharTableData: [],
      mainCharTableData: [],
      tableRendered: false,
      connectionDataFetchDone: false,
      notEnoughDataForConnections: false,
      forceGraphForceAplied: false,
    };
  }

  /**
   * This method obtains the data regarding the imagecounts per year for the copyright defined by the given parameter
   * The data is then passed into the options required by echarts and returned
   */
  fetchYearData = async (param) => {
    let yearData = await (
      await axios.get(
        this.baseUrl + "copyright/year/" + encodeURIComponent(param)
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
   * This method obtains the data regarding the imagecounts per month for the copyright defined by the given parameter
   * The data is then passed into the options required by echarts and returned
   */
  fetchMonthData = async (param) => {
    let monthChartData = await (
      await axios.get(
        this.baseUrl + "copyright/month/" + encodeURIComponent(param)
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
   * This method obtains the data regarding the imagecounts per day for the copyright defined by the given parameter in the last 31 days
   * The data is then passed into the options required by echarts and returned
   */
  fetchDayData = async (param) => {
    let dayChartData = await (
      await axios.get(
        this.baseUrl + "copyright/day/" + encodeURIComponent(param)
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
   * This method obtains up to 5 urls showing the latest images uploaded for the given copyright
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
   * This method obtains and returns the amount of images uploaded for the given copyright
   */
  fetchImageCount = async (param) => {
    let total = await (
      await axios.get(
        this.baseUrl + "copyright/total/" + encodeURIComponent(param)
      )
    ).data;

    return Object.values(total[0])[0];
  };

  /**
   * This method obtains and returns the total average score given images containing the given copyright
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
   * This method obtains the data regarding the average scores per year that were given to images containing the given copyright
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
   * This method obtains the data regarding the average scores per month that were given to images containing the given copyright
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
   * This method obtains and returns information regarding implications as an array
   * The first field of the array containing information regarding the copyright from which the given copyright originated
   * If that field is empty the copyright is the mainCopyright
   * The second field of the array contains information regarding copyrights that originate from the given copyright
   * (this field is only filled when the copyright doesnt originate from another copyright)
   */
  fetchImplications = async (param) => {
    let implications = await (
      await axios.get(
        this.baseUrl + "copyright/implications/" + encodeURIComponent(param)
      )
    ).data;
    return implications;
  };

  /**
   * This method obtains and returns a list of copyrights that the given copyright was tagged together with and a count of occurrences
   */
  fetchCopyrightPairings = async (param) => {
    let copyrightList = await (
      await axios.get(
        this.baseUrl + "copyright/copyrightPairing/" + encodeURIComponent(param)
      )
    ).data;

    let tableData = copyrightList.map((entry) => [
      Object.values(entry)[0],
      Object.keys(entry).toString(),
    ]);

    return tableData;
  };

  /**
   * This method obtains data regarding copyrights that the given copyrights was tagged together with that is used to create a piechart
   * The first 19 entries are detailed and the rest is automatically grouped
   * The data is then passed into the options required by echarts and returned
   */
  fetchCopyrightPairingsPieData = async (param) => {
    let copyrightPieChart = await (
      await axios.get(
        this.baseUrl +
          "copyright/copyrightPairingPie/" +
          encodeURIComponent(param)
      )
    ).data;

    let copyrightPieData = {
      type: "pie",
      name: "Copyright Pairing Data",
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
        text: "Total Connections with the given Copyright",
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
   * This method obtains the data regarding the connections between the top 100 characters
   * that have the given copyright as their maincopyright and the nodes
   * 3 values are returned
   * 1 - the average value of the top 5 connections
   * 2 - the nodes with the following values (id,name,count,group)
   * 3 - the links in the graph with the following values (source,taget,distance)
   */
  fetchCharConnectionData = async (param) => {
    let data = await (
      await axios.get(
        this.baseUrl +
          "copyright/characterCombinations/" +
          encodeURIComponent(param)
      )
    ).data;

    let nodes = data._2.map((elem) => ({
      id: elem[Object.keys(elem)[0]],
      name: elem[Object.keys(elem)[0]].toString().replaceAll("_", " "),
      count: elem[Object.keys(elem)[1]],
      group: elem[Object.keys(elem)[2]],
    }));

    let links = data._3.map((elem) => ({
      source: elem[Object.keys(elem)[0]],
      target: elem[Object.keys(elem)[1]],
      distance: elem[Object.keys(elem)[2]],
    }));

    return [data._1, nodes, links];
  };

  /**
   * This method obtains and returns a list of characters that the given copyright was drawn together with and a count of occurrences
   */
  fetchAllCharacters = async (param) => {
    let allCharacterList = await (
      await axios.get(
        this.baseUrl + "copyright/allCharacters/" + encodeURIComponent(param)
      )
    ).data;

    let tableData = allCharacterList.map((entry) => [
      Object.values(entry)[0],
      Object.keys(entry).toString(),
    ]);

    return tableData;
  };

  /**
   * This method obtains data regarding characters that the given copyright was drawn together with that is used to create a piechart
   * The first 19 entries are detailed and the rest is automatically grouped
   * The data is then passed into the options required by echarts and returned
   */
  fetchAllCharPieData = async (param) => {
    let characterPieChart = await (
      await axios.get(
        this.baseUrl + "copyright/allCharactersPie/" + encodeURIComponent(param)
      )
    ).data;

    let characterPieData = {
      type: "pie",
      name: "Character Data",
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
        text: "Total Character Count",
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
   * This method obtains and returns a list of characters that have the given copyright as their maincopyright
   * together with and a count of occurrences
   */
  fetchMainCharacters = async (param) => {
    let mainCharacterList = await (
      await axios.get(
        this.baseUrl + "copyright/mainCharacters/" + encodeURIComponent(param)
      )
    ).data;

    let tableData = mainCharacterList.map((entry) => [
      Object.values(entry)[0],
      Object.keys(entry).toString(),
    ]);

    return tableData;
  };

  /**
   * This method obtains and returns data regarding characters that have the given copyright as their maincopyright
   * that is used to create a piechart
   * The first 19 entries are detailed and the rest is automatically grouped
   * The data is then passed into the options required by echarts and returned
   */
  fetchMainCharPieData = async (param) => {
    let characterPieChart = await (
      await axios.get(
        this.baseUrl +
          "copyright/mainCharactersPie/" +
          encodeURIComponent(param)
      )
    ).data;

    let characterPieData = {
      type: "pie",
      name: "Main Character Data",
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
        text: "Total Main Character Count",
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
   * This method obtains the data regarding the imagecounts per year for the top 8 characters
   * that have the given copyright as their maincopyright
   * The data is then passed into the options required by echarts and returned
   */
  fetchTopCharactersYear = async (param) => {
    let yearData = await (
      await axios.get(
        this.baseUrl +
          "copyright/topCharactersYear/" +
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
        text: "Top Characters per year",
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
   * This method obtains the data regarding the imagecounts per month for the top 8 characters
   * that have the given copyright as their maincopyright
   * The data is then passed into the options required by echarts and returned
   */
  fetchTopCharactersMonth = async (param) => {
    let monthChartData = await (
      await axios.get(
        this.baseUrl +
          "copyright/topCharactersMonth/" +
          encodeURIComponent(param)
      )
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
        text: "Top Characters per Month",
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
   * This method obtains the data regarding the imagecounts per day for the top 8 characters in the last 31 days
   * that have the given copyright as their maincopyright
   * The data is then passed into the options required by echarts and returned
   */
  fetchTopCharactersDay = async (param) => {
    let dayChartData = await (
      await axios.get(
        this.baseUrl + "copyright/topCharactersDay/" + encodeURIComponent(param)
      )
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
        text: "Top Characters per Day",
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

  /**
   * This method is called when a component is initially rendered here all the data is obtained
   */
  componentDidMount = async () => {
    //Thank you okina baba for giving us a series with the name "kumo desu ga nani ka?"
    //-> the ? requires special treatment
    let param = (this.props.match.params.copyright + this.props.location.search)
      .split(" ")
      .join("")
      .replace("%2F", "/");

    try {
      if (!this.state.copyright) {
        await axios.get(this.baseUrl + "endCurrentJobs");
        this.props.refetchRoutes();
      }

      if (this.state.copyright !== param) {
        await axios.get(this.baseUrl + "endCurrentJobs");
        this.props.refetchRoutes();

        this.setState({
          copyright: param,
          imageData: null,
          yearChartData: null,
          monthChartData: null,
          dayChartData: null,
          avgUpScore: null,
          avgUpScoreYearData: null,
          avgUpScoreMonthData: null,
          copyrightConnectionPieData: null,
          imageCount: null,
          allCharPieData: null,
          mainCharPieData: null,
          topCharactersYearData: null,
          topCharactersMonthData: null,
          topCharactersDayData: null,
          nodes: null,
          links: null,
          copyrightConnectionTableData: [],
          charConnectionTableData: [],
          allCharTableData: [],
          mainCharTableData: [],
          tableRendered: false,
          connectionDataFetchDone: false,
          notEnoughDataForConnections: false,
          forceGraphForceAplied: false,
        });
      }

      let [
        year,
        imageData,
        imageCount,
        implications,
        avgUpScore,
        avgUpScoreYearData,
        copyConnectionPieData,
        copyConnectionData,
        allCharPieData,
        allCharData,
        mainCharData,
        mainCharPieData,
        topCharDayData,
      ] = await Promise.all([
        this.fetchYearData(param),
        this.fetchLatestImages(param),
        this.fetchImageCount(param),
        this.fetchImplications(param),
        this.fetchAvgUpScore(param),
        this.fetchAvgUpScoreYear(param),
        this.fetchCopyrightPairingsPieData(param),
        this.fetchCopyrightPairings(param),
        this.fetchAllCharPieData(param),
        this.fetchAllCharacters(param),
        this.fetchMainCharacters(param),
        this.fetchMainCharPieData(param),
        this.fetchTopCharactersDay(param),
      ]);

      this.setState({
        copyright: param,
        yearChartData: year,
        imageData: imageData,
        imageCount: imageCount,
        avgUpScore: avgUpScore,
        avgUpScoreYearData: avgUpScoreYearData,
        originating: implications._1,
        implications: implications._2,
        copyrightConnectionPieData: copyConnectionPieData,
        copyrightConnectionTableData: copyConnectionData,
        allCharPieData: allCharPieData,
        allCharTableData: allCharData,
        mainCharTableData: mainCharData,
        mainCharPieData: mainCharPieData,
        topCharactersDayData: topCharDayData,
      });

      let [month, day, avgUpScoreMonthData, forceData] = await Promise.all([
        this.fetchMonthData(param),
        this.fetchDayData(param),
        this.fetchAvgUpScoreMonth(param),
        this.fetchCharConnectionData(param),
      ]);

      if (forceData[0] === 0) {
        this.setState({
          monthChartData: month,
          dayChartData: day,
          avgUpScoreMonthData: avgUpScoreMonthData,
          connectionDataFetchDone: true,
          notEnoughDataForConnections: true,
        });
      } else {
        this.setState(
          {
            monthChartData: month,
            dayChartData: day,
            avgUpScoreMonthData: avgUpScoreMonthData,
            nodes: forceData[1],
          },
          () => {
            if (this.state.tableRendered !== true) {
              let temp = forceData[2].map((entry) => [
                entry.source,
                entry.target,
                entry.distance,
              ]);

              this.setState({
                charConnectionTableData: temp,
                tableRendered: true,
              });
            }

            this.setState({
              links: forceData[2],
              maxCharCount: forceData[1][0].val,
              maxConnectionCount: forceData[0],
            });
          }
        );
      }

      if (this.fgRef.current) {
        if (!this.state.forceGraphForceAplied) {
          this.fgRef.current
            .d3Force("link")
            .distance(
              (link) =>
                Math.pow(link.distance, -1) *
                (this.state.maxConnectionCount * 10)
            );

          this.setState({ forceGraphForceAplied: true });
        }
      }

      let [topCharYearData, topCharMonthData] = await Promise.all([
        this.fetchTopCharactersYear(param),
        this.fetchTopCharactersMonth(param),
      ]);

      this.setState({
        topCharactersYearData: topCharYearData,
        topCharactersMonthData: topCharMonthData,
      });
      // let topCharYearData = await this.fetchTopCharactersYear();
      // this.setState({ topCharactersYearData: topCharYearData });

      // let topCharMonthData = await this.fetchTopCharactersMonth();

      // this.setState({ topCharactersMonthData: topCharMonthData });
    } catch (error) {
      console.log(error);
    }
  };

  /**
   * This method is called when the component is updated/a change takes place
   * Whenever this happens a check is executred to see if the copyright parameter changed
   * If that is the case the site for a new copyright was called and thus the new information should be obtained
   * This is done by calling componentDidMount where all the data is obtained
   */
  componentDidUpdate = async () => {
    if (
      this.state.copyright !==
      (this.props.match.params.copyright + this.props.location.search)
        .split(" ")
        .join("")
        .replace("%2F", "/")
    ) {
      this.componentDidMount();
    }
  };

  render() {
    let copyList = this.props.copyList;

    let type = (this.props.match.params.copyright + this.props.location.search)
      .split(" ")
      .join("")
      .replace("%2F", "/");
    if (!copyList.includes(type)) {
      this.props.history.push("/error");
    }

    return (
      <div>
        <h1>Copyright - {type}</h1>

        <BreakLine />

        <h3>General Information:</h3>

        <div className="d-flex justify-content-center">
          {this.state.imageCount ? (
            <div>
              <h3>
                This copyright has {this.state.imageCount} images in total.
              </h3>

              <br />

              {this.state.originating.length > 0 ? (
                <h5>
                  {" "}
                  This copyright originates from the following copyright :
                  <Link
                    style={{ color: "purple", fontSize: "20px" }}
                    to={"/copyright/" + this.state.originating[0]}
                  >
                    {this.state.originating[0]}
                  </Link>
                </h5>
              ) : null}

              {this.state.implications.length > 0 ? (
                <h5>
                  {" "}
                  In total {this.state.implications.length} copyright(s)
                  originate(s) from this copyright :
                  <Accordion>
                    <Accordion.Item eventKey="0">
                      <Accordion.Header>Related Copyrights</Accordion.Header>
                      <Accordion.Body>
                        <div
                          className="table-responsive"
                          style={{ height: "250px" }}
                        >
                          <Table>
                            <thead>
                              <tr>
                                <th>Copyright</th>
                              </tr>
                            </thead>
                            <tbody>
                              {this.state.implications.map((entry, index) => {
                                return (
                                  <tr key={index}>
                                    <Link
                                      style={{
                                        color: "purple",
                                        fontSize: "20px",
                                      }}
                                      to={"/copyright/" + entry}
                                    >
                                      {entry}
                                    </Link>
                                  </tr>
                                );
                              })}
                            </tbody>
                          </Table>
                        </div>
                      </Accordion.Body>
                    </Accordion.Item>{" "}
                  </Accordion>
                </h5>
              ) : null}

              {this.state.copyrightConnectionTableData.length > 0 ? (
                <h5>
                  This copyright was drawn together with{" "}
                  {this.state.copyrightConnectionTableData.length} other
                  copyrigth(s).
                </h5>
              ) : null}

              {this.state.allCharTableData.length > 0 ? (
                <h5>
                  This copyright contains images with{" "}
                  {this.state.allCharTableData.length} different characters in
                  total.
                </h5>
              ) : null}
              {this.state.mainCharTableData.length > 0 &&
              this.state.allCharTableData.length > 0 ? (
                <h5>
                  Of those {this.state.mainCharTableData.length} characters have
                  this or the parent copyright as their main copyright.
                </h5>
              ) : null}

              {this.state.avgUpScore ? (
                <h5>
                  The average Score for this Copyright is{" "}
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
            the given year/month/day for the given copyright.
            <br />
            If no image was uploaded for the given copyright in the given
            year/month/day a 0 is displayed.
            <br />
            In order to change between the different tables, please click on the
            Tab-Selector at the top of the graphic
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
          <h3>Connections to other Copyrights</h3>

          <p>
            Here you can see a piechart displaying the connections from this
            copyright to other copyrights. The first 19 entries are detailed and
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
            {this.state.copyrightConnectionPieData ? (
              <EChart
                options={this.state.copyrightConnectionPieData}
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
                      {this.state.copyrightConnectionTableData.map(
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
          <h3>Character Occurrences in the given Copyright</h3>
          <p>
            Here you can find information about characters that were tagged
            together with this copyright and their occurences in it.
            <br />
            <br />
            In the first tab, you can see a piechart displaying the occurrences
            of different characters in this copyright. The first 19 entries are
            detailed and the rest is grouped together in order to not clutter
            the graph. You can find detailed information regarding the
            occurences in the table below the graph.
            <br />
            <br />
            In the second tab, you can once again see a piechart displaying the
            occurrences of different characters in the copyright. This time
            however, only characters are included that have the given copyright
            as their main copyright. The first 19 entries are detailed and the
            rest is grouped together in order to not clutter the graph. You can
            find detailed information regarding the occurences in the table
            below the graph.
            <br />
            <br />
            In the 3rd, 4th and the 5th tab you can find information about the
            amount of images uploaded in the given year/month/day for the 8 most
            popular characters in the given copyright.
          </p>
        </div>

        <div>
          <Tabs
            defaultActiveKey="all"
            id="uncontrolled-tab-example"
            className="mb-3"
          >
            <Tab eventKey="all" title="All Characters">
              <div
                className="d-flex justify-content-center align-items-center"
                style={{ minWidth: "100%", height: 500 }}
              >
                {this.state.allCharPieData ? (
                  <EChart
                    options={this.state.allCharPieData}
                    resizeObserver={resizeObserver}
                  />
                ) : (
                  <DatafetchAnimation />
                )}
              </div>
              <Accordion>
                <Accordion.Item eventKey="0">
                  <Accordion.Header>All Character Table</Accordion.Header>
                  <Accordion.Body>
                    <div
                      className="table-responsive"
                      style={{ height: "250px" }}
                    >
                      <Table>
                        <thead>
                          <tr>
                            <th>Count</th>
                            <th>Character</th>
                          </tr>
                        </thead>
                        <tbody>
                          {this.state.allCharTableData.map((entry, index) => {
                            return (
                              <tr key={index}>
                                <td>{entry[0]}</td>
                                <td>
                                  <Link
                                    style={{
                                      color: "green",
                                      fontSize: "20px",
                                    }}
                                    to={"/character/" + entry[1]}
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
            </Tab>

            <Tab eventKey="main" title="Main Characters">
              <div
                className="d-flex justify-content-center align-items-center"
                style={{ minWidth: "100%", height: 500 }}
              >
                {this.state.mainCharPieData ? (
                  <EChart
                    options={this.state.mainCharPieData}
                    resizeObserver={resizeObserver}
                  />
                ) : (
                  <DatafetchAnimation />
                )}
              </div>
              <Accordion>
                <Accordion.Item eventKey="0">
                  <Accordion.Header>Main Character Table</Accordion.Header>
                  <Accordion.Body>
                    <div
                      className="table-responsive"
                      style={{ height: "250px" }}
                    >
                      <Table>
                        <thead>
                          <tr>
                            <th>Count</th>
                            <th>Character</th>
                          </tr>
                        </thead>
                        <tbody>
                          {this.state.mainCharTableData.map((entry, index) => {
                            return (
                              <tr key={index}>
                                <td>{entry[0]}</td>
                                <td>
                                  <Link
                                    style={{
                                      color: "green",
                                      fontSize: "20px",
                                    }}
                                    to={"/character/" + entry[1]}
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
            </Tab>

            <Tab eventKey="years" title="Top 8 Characters per Year">
              <div
                className="d-flex justify-content-center align-items-center"
                style={{ minWidth: "100%", height: 500 }}
              >
                {this.state.topCharactersYearData ? (
                  <EChart
                    options={this.state.topCharactersYearData}
                    resizeObserver={resizeObserver}
                  />
                ) : (
                  <DatafetchAnimation />
                )}
              </div>
            </Tab>

            <Tab eventKey="month" title="Top 8 Characters per Month">
              <div
                className="d-flex justify-content-center align-items-center"
                style={{ minWidth: "100%", height: 500 }}
              >
                {this.state.topCharactersMonthData ? (
                  <EChart
                    options={this.state.topCharactersMonthData}
                    resizeObserver={resizeObserver}
                  />
                ) : (
                  <DatafetchAnimation />
                )}
              </div>
            </Tab>

            <Tab eventKey="day" title="Top 8 Characters in the last Month">
              <div
                className="d-flex justify-content-center align-items-center"
                style={{ minWidth: "100%", height: 500 }}
              >
                {this.state.topCharactersDayData ? (
                  <EChart
                    options={this.state.topCharactersDayData}
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
          <h3>Average score per Year and Month </h3>
          <p>
            Here you can see a graph displaying the evolution of the average
            score of all images of the given copyright.
            <br />
            If no score was given for the given copyright in the given
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

        <BreakLine />

        <div>
          <h3>Force-directed graph of character combinations</h3>
          <p>
            Here you can see a force-directed graph displaying the connections
            between the 100 most common characters in the given copyright. A
            connection between two characters is generated when an image was
            tagged with both those characters.
          </p>
          <p>
            The thickness of the line between two characters indicates how many
            connections there are. The thicker a line is, the more often those
            two characters were drawn together.
            <br />
            The number behind the name of a node indicates how many total images
            with the given character exist.
            <br />
            The closer a character is to the centre, the more often it was
            tagged together with others in general.
            <br />
            The farther a character is away from the center, the less often it
            was tagged together with other characters in general.
            <br />
            Sometimes multiple clusters can generate from characters. This can
            be an indicator of subgroups within the copyright e.g., a
            different/opposing factions etc. or different origins within the
            copyright e.g., different games etc.
          </p>
          <p>
            In some cases the initial force is not applied correctly e.g., if
            the window is not open when the site is re-rendered with the data
            regarding the links. To solve this issue, please click inside the
            window of the force graph on one of the nodes and then the force
            should be reapplied.
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
          {this.state.links || this.state.connectionDataFetchDone ? (
            <div>
              {this.state.notEnoughDataForConnections ? (
                <h2>
                  There is not enough data regarding connections to generate a
                  force graph.
                </h2>
              ) : (
                <div>
                  <ForceGraph3D
                    warmupTicks={100}
                    cooldownTicks={0}
                    ref={this.fgRef}
                    height={500}
                    nodeCanvasObjectMode={() => "after"}
                    width={this.parentFigRef.current.offsetWidth}
                    nodeAutoColorBy="group"
                    nodeThreeObject={(node) => {
                      const sprite = new SpriteText(
                        node.name
                          .split(" ")
                          .map(
                            (word) =>
                              word.charAt(0).toUpperCase() + word.slice(1)
                          )
                          .join(" ") +
                          " (" +
                          node.count +
                          ")"
                      );
                      sprite.color = node.color;
                      sprite.textHeight = Math.log(node.count) / Math.log(5);
                      return sprite;
                    }}
                    nodeThreeObjectExtend={true}
                    nodeRelSize={0}
                    onNodeClick={(node) => this.nodeClicked(node)}
                    linkWidth={(link) =>
                      (link.distance ^ 2) *
                      (1 / (this.state.maxConnectionCount / 7.5))
                    }
                    linkColor={(link) =>
                      "rgba(255, 255, 255, " +
                      (link.distance ^ 2) *
                        (1 / (this.state.maxConnectionCount / 7.5)) +
                      ")"
                    }
                    graphData={{
                      nodes: this.state.nodes,
                      links: this.state.links,
                    }}
                  />

                  <br></br>

                  <Accordion>
                    <Accordion.Item eventKey="0">
                      <Accordion.Header>
                        Character Connection Table
                      </Accordion.Header>
                      <Accordion.Body>
                        <div
                          className="table-responsive"
                          style={{ height: "250px" }}
                        >
                          <Table>
                            <thead>
                              <tr>
                                <th>Character-1</th>
                                <th>Character-2</th>
                                <th>Count</th>
                              </tr>
                            </thead>
                            <tbody>
                              {this.state.charConnectionTableData.map(
                                (entry, index) => {
                                  return (
                                    <tr key={index}>
                                      <td>
                                        <Link
                                          style={{
                                            color: "purple",
                                            fontSize: "20px",
                                          }}
                                          to={"/character/" + entry[0]}
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
                                          to={"/character/" + entry[1]}
                                        >
                                          {entry[1]}
                                        </Link>
                                      </td>
                                      <td>{entry[2]}</td>
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
              )}
            </div>
          ) : (
            <DatafetchAnimation />
          )}
        </div>
      </div>
    );
  }
}

export default withRouter(CopyrightPage);
