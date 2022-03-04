import React from "react";

import EChart from "../EChart/EChart";
import resizeObserver from "../EChart/resizeObserver";
import DatafetchAnimation from "../Datafetchanimation/DatafetchAnimation";
import axios from "axios";
import { Tab, Tabs, Accordion, Table } from "react-bootstrap";
import { Link } from "react-router-dom";
import BreakLine from "../BreakLine/BreakLine";

class ClothingPage extends React.Component {
  constructor(props) {
    super(props);
    this.baseUrl = "http://safeana.f4.htw-berlin.de:8080/";
    //this.baseUrl = "http://localhost:8080/"

    this.state = {
      tableData_HeadwearAndHeadgear: [],
      tableData_ShirtsAndTopwear: [],
      tableData_PantsAndBottomwear: [],
      tableData_LegsAndFootwear: [],
      tableData_ShoesAndFootwear: [],
      tableData_UniformsAndCostumes: [],
      tableData_SwimsuitsAndBodysuits: [],
      tableData_TraditionalClothing: [],

      pieOption_HeadwearAndHeadgear: null,
      pieOption_ShirtsAndTopwear: null,
      pieOption_PantsAndBottomwear: null,
      pieOption_LegsAndFootwear: null,
      pieOption_ShoesAndFootwear: null,
      pieOption_UniformsAndCostumes: null,
      pieOption_SwimsuitsAndBodysuits: null,
      pieOption_TraditionalClothing: null,

      yearChartData_HeadwearAndHeadgear: null,
      yearChartData_ShirtsAndTopwear: null,
      yearChartData_PantsAndBottomwear: null,
      yearChartData_LegsAndFootwear: null,
      yearChartData_ShoesAndFootwear: null,
      yearChartData_UniformsAndCostumes: null,
      yearChartData_SwimsuitsAndBodysuits: null,
      yearChartData_TraditionalClothing: null,

      monthChartData_HeadwearAndHeadgear: null,
      monthChartData_ShirtsAndTopwear: null,
      monthChartData_PantsAndBottomwear: null,
      monthChartData_LegsAndFootwear: null,
      monthChartData_ShoesAndFootwear: null,
      monthChartData_UniformsAndCostumes: null,
      monthChartData_SwimsuitsAndBodysuits: null,
      monthChartData_TraditionalClothing: null,
    };
  }

  fetchTotalData = async (groupName, name) => {
    let totalData = await (
      await axios.get(this.baseUrl + "tagGroups/" + groupName + "/total")
    ).data;

    let tableData = totalData.map((entry) => [
      Object.values(entry)[0],
      Object.keys(entry).toString(),
    ]);

    let pieData = {
      type: "pie",
      name: "Tag Data",
      data: totalData
        .map((entry) => [
          {
            value: Object.values(entry)[0],
            name: Object.keys(entry).toString(),
          },
        ])
        .flat(),
    };

    console.log(name + " Group Image Count");

    let pieChartOption = {
      grid: {
        right: "15%",
      },
      title: {
        text: name + " Group Image Count",
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
      series: pieData,
    };

    return [tableData, pieChartOption];
  };

  fetchYearData = async (groupName) => {
    let yearData = await (
      await axios.get(this.baseUrl + "tagGroups/" + groupName + "/year")
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
        text: "Images per Year",
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

  fetchMonthData = async (groupName) => {
    let monthData = await (
      await axios.get(this.baseUrl + "tagGroups/" + groupName + "/month")
    ).data;

    let monthSeriesData = Object.entries(monthData._3)
      .map((entry) =>
        Object.entries(Object.entries(entry)[1][1]).map(([key, value]) => ({
          name: key,
          type: "line",
          data: value.map((e) => [Object.keys(e)[0], Object.values(e)[0]]),
        }))
      )
      .flat();

    let monthDataDescription = Object.entries(monthData._3)
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
        min: monthData._1.toString(),
        max: monthData._2.toString(),
        boundaryGap: false,
      },
      yAxis: {},
      series: monthSeriesData,
    };
    return monthChartOption;
  };

  componentDidMount = async () => {
    try {
      await axios.get(this.baseUrl + "endCurrentJobs");
      this.props.refetchRoutes();

      let [
        totalHeadwearAndHeadgear,
        totalShirtsAndTopwear,
        totalPantsAndBottomwear,
        totalLegsAndFootwear,
        totalShoesAndFootwear,
        totalUniformsAndCostumes,
        totalSwimsuitsAndBodysuits,
        totalTraditionalClothing,
      ] = await Promise.all([
        this.fetchTotalData("headwearAndHeadgear", "Headwear and Headgeat"),
        this.fetchTotalData("shirtsAndTopwear", "Shirts and Topwear"),
        this.fetchTotalData("pantsAndBottomwear", "Pants and Bottomwear"),
        this.fetchTotalData("legsAndFootwear", "Legs and Footwear"),
        this.fetchTotalData("shoesAndFootwear", "Shoes and Footwear"),
        this.fetchTotalData("uniformsAndCostumes", "Uniforms and Cosumtes"),
        this.fetchTotalData("swimsuitsAndBodysuits", "Swimsuits and Bodysuits"),
        this.fetchTotalData("traditionalClothing", "Traditional Clothing"),
      ]);

      this.setState({
        tableData_HeadwearAndHeadgear: totalHeadwearAndHeadgear[0],
        tableData_ShirtsAndTopwear: totalShirtsAndTopwear[0],
        tableData_PantsAndBottomwear: totalPantsAndBottomwear[0],
        tableData_LegsAndFootwear: totalLegsAndFootwear[0],
        tableData_ShoesAndFootwear: totalShoesAndFootwear[0],
        tableData_UniformsAndCostumes: totalUniformsAndCostumes[0],
        tableData_SwimsuitsAndBodysuits: totalSwimsuitsAndBodysuits[0],
        tableData_TraditionalClothing: totalTraditionalClothing[0],

        pieOption_HeadwearAndHeadgear: totalHeadwearAndHeadgear[1],
        pieOption_ShirtsAndTopwear: totalShirtsAndTopwear[1],
        pieOption_PantsAndBottomwear: totalPantsAndBottomwear[1],
        pieOption_LegsAndFootwear: totalLegsAndFootwear[1],
        pieOption_ShoesAndFootwear: totalShoesAndFootwear[1],
        pieOption_UniformsAndCostumes: totalUniformsAndCostumes[1],
        pieOption_SwimsuitsAndBodysuits: totalSwimsuitsAndBodysuits[1],
        pieOption_TraditionalClothing: totalTraditionalClothing[1],
      });

      let [
        yearHeadwearAndHeadgear,
        yearShirtsAndTopwear,
        yearPantsAndBottomwear,
        yearLegsAndFootwear,
        yearShoesAndFootwear,
        yearUniformsAndCostumes,
        yearSwimsuitsAndBodysuits,
        yearTraditionalClothing,
      ] = await Promise.all([
        this.fetchYearData("headwearAndHeadgear"),
        this.fetchYearData("shirtsAndTopwear"),
        this.fetchYearData("pantsAndBottomwear"),
        this.fetchYearData("legsAndFootwear"),
        this.fetchYearData("shoesAndFootwear"),
        this.fetchYearData("uniformsAndCostumes"),
        this.fetchYearData("swimsuitsAndBodysuits"),
        this.fetchYearData("traditionalClothing"),
      ]);

      this.setState({
        yearChartData_HeadwearAndHeadgear: yearHeadwearAndHeadgear,
        yearChartData_ShirtsAndTopwear: yearShirtsAndTopwear,
        yearChartData_PantsAndBottomwear: yearPantsAndBottomwear,
        yearChartData_LegsAndFootwear: yearLegsAndFootwear,
        yearChartData_ShoesAndFootwear: yearShoesAndFootwear,
        yearChartData_UniformsAndCostumes: yearUniformsAndCostumes,
        yearChartData_SwimsuitsAndBodysuits: yearSwimsuitsAndBodysuits,
        yearChartData_TraditionalClothing: yearTraditionalClothing,
      });

      let [
        monthHeadwearAndHeadgear,
        monthShirtsAndTopwear,
        monthPantsAndBottomwear,
        monthLegsAndFootwear,
        monthShoesAndFootwear,
        monthUniformsAndCostumes,
        monthSwimsuitsAndBodysuits,
        monthTraditionalClothing,
      ] = await Promise.all([
        this.fetchMonthData("headwearAndHeadgear"),
        this.fetchMonthData("shirtsAndTopwear"),
        this.fetchMonthData("pantsAndBottomwear"),
        this.fetchMonthData("legsAndFootwear"),
        this.fetchMonthData("shoesAndFootwear"),
        this.fetchMonthData("uniformsAndCostumes"),
        this.fetchMonthData("swimsuitsAndBodysuits"),
        this.fetchMonthData("traditionalClothing"),
      ]);

      this.setState({
        monthChartData_HeadwearAndHeadgear: monthHeadwearAndHeadgear,
        monthChartData_ShirtsAndTopwear: monthShirtsAndTopwear,
        monthChartData_PantsAndBottomwear: monthPantsAndBottomwear,
        monthChartData_LegsAndFootwear: monthLegsAndFootwear,
        monthChartData_ShoesAndFootwear: monthShoesAndFootwear,
        monthChartData_UniformsAndCostumes: monthUniformsAndCostumes,
        monthChartData_SwimsuitsAndBodysuits: monthSwimsuitsAndBodysuits,
        monthChartData_TraditionalClothing: monthTraditionalClothing,
      });
    } catch (error) {}
  };

  renderTabsData = (tableData, pieData, yearData, monthData) => {
    return (
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
            {pieData ? (
              <EChart options={pieData} resizeObserver={resizeObserver} />
            ) : (
              <DatafetchAnimation />
            )}
          </div>
          <Accordion>
            <Accordion.Item eventKey="0">
              <Accordion.Header>Tag Count Table</Accordion.Header>
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
                      {tableData.map((entry, index) => {
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
            </Accordion.Item>
          </Accordion>
        </Tab>

        <Tab eventKey="year" title="Year" className="mb-3">
          <div
            className="d-flex justify-content-center align-items-center"
            style={{ width: "100%", height: 500 }}
          >
            {yearData ? (
              <EChart options={yearData} resizeObserver={resizeObserver} />
            ) : (
              <DatafetchAnimation />
            )}
          </div>
        </Tab>

        <Tab eventKey="month" title="Month" className="mb-3">
          <div
            className="d-flex justify-content-center align-items-center"
            style={{ width: "100%", height: 500 }}
          >
            {monthData ? (
              <EChart options={monthData} resizeObserver={resizeObserver} />
            ) : (
              <DatafetchAnimation />
            )}
          </div>
        </Tab>
      </Tabs>
    );
  };

  render() {
    return (
      <div>
        <div>
          <h1>Clothing Tag Groups</h1>

          <p>
            This is a page where you can find information about the different
            tag groups associated with clothing that can be found in the images.
            <br />
            There are 8 different overall tag groups associated with clothing.
            In those overall tag groups, some tags have dedicated subgroups in
            order to describe more accurately the piece of clothing shown in the
            image. An example of that would be the "school_uniform" tag which in
            itself forms a group consisting of "serafuka", "gakuran" and the
            "meiji_school_uniform" tag. Those subgroups however are not
            displayed in order not to clutter the graphic.
            <br />
            For each tag group, a piechart exists showing the composition of the
            given tag group. This piechart is shown on the first tab by default.
            In the other 2 tabs, you can find information regarding the
            imagecounts for the top 8 tags in the current tag group by month and
            year. In some case with the month overview, some recurring patterns
            can be seen.
          </p>
        </div>

        <BreakLine />

        <div>
          <h3>Headwear and Headgear</h3>
          <p>
            This tag group consists of hats and other clothing items worn on the
            head. As you can see, hats, hair ribbons and hair bow make up around
            75% of all entries in this tag group.
          </p>
        </div>

        {this.renderTabsData(
          this.state.tableData_HeadwearAndHeadgear,
          this.state.pieOption_HeadwearAndHeadgear,
          this.state.yearChartData_HeadwearAndHeadgear,
          this.state.monthChartData_HeadwearAndHeadgear
        )}

        <BreakLine />

        <div>
          <h3>Shirts and Topwear</h3>
          <p>
            This tag group consists of clothing that is worn on the upper part
            of the body. As you can see, dresses, shirts, and jackets make up
            around 75% of all entries in this group. In the month view, you can
            a somewhat recurring patterns for the sash tag and that there is
            usually a spike in January.
            <br />
            This is probably related to the Japanese tradition of Hatsumōde
            (初詣) where people visit a Shinto or Buddhist shrine for the first
            time after the Japanese new year. Some people do this visit in
            traditional Japanese clothing, where a sash (in that case an obi -
            帯) is also included. Another influence could be the so-called
            "Coming of Age Day" (seijin no hi - 成人の日) which is held every
            year on the second monday of January in order to congratulate and
            celebrate those that have reached the age of maturity. On this day
            typically traditional clothing is worn.
          </p>
        </div>

        {this.renderTabsData(
          this.state.tableData_ShirtsAndTopwear,
          this.state.pieOption_ShirtsAndTopwear,
          this.state.yearChartData_ShirtsAndTopwear,
          this.state.monthChartData_ShirtsAndTopwear
        )}

        <BreakLine />

        <div>
          <h3>Pants and Bottomwear</h3>
          <p>
            This tag group consists of clothing that is worn on the lower half
            ot the body. As you can see, skirts make up around 70% of all
            entries in this tag group. Together with shorts and pants, they make
            up around 95% of all entries.
          </p>
        </div>

        {this.renderTabsData(
          this.state.tableData_PantsAndBottomwear,
          this.state.pieOption_PantsAndBottomwear,
          this.state.yearChartData_PantsAndBottomwear,
          this.state.monthChartData_PantsAndBottomwear
        )}

        <BreakLine />

        <div>
          <h3>Legs and Footwear</h3>
          <p>
            This tag group consists of clothing that is worn on the legs and
            pantyhoses. As you can see, thighigh socks make up more than half of
            the entries in this category. Together with pantyhoses they make up
            more than 75% of all entries.
          </p>
        </div>

        {this.renderTabsData(
          this.state.tableData_LegsAndFootwear,
          this.state.pieOption_LegsAndFootwear,
          this.state.yearChartData_LegsAndFootwear,
          this.state.monthChartData_LegsAndFootwear
        )}

        <BreakLine />

        <div>
          <h3>Shoes and Footwear</h3>
          <p>
            This tag group consists of different types of footwear. As you can
            see, boots make up around 50% of all entries. Together with high
            heels and sandals, they account for around 75% of all entries in
            this group.
            <br />
            In the month view you can a somewhat recurring patterns for the
            sandals tag and how the entries for this tag usually peak in the
            summer months when it is warm and January where they are worn
            together with traditional clothes.
          </p>
        </div>

        {this.renderTabsData(
          this.state.tableData_ShoesAndFootwear,
          this.state.pieOption_ShoesAndFootwear,
          this.state.yearChartData_ShoesAndFootwear,
          this.state.monthChartData_ShoesAndFootwear
        )}

        <BreakLine />

        <div>
          <h3>Uniforms and Costumes</h3>
          <p>
            This tag group consists of different costumes and uniforms. As you
            can see, school uniforms are very dominant with around 45% of all
            entries coming from this tag. This is probably caused by the fact
            that school uniforms are usually mandatory in Japan, and this is
            also reflected in the pictures.
            <br />
            In the month view, you can see a recurring pattern for the santa
            costume and how the entries of this tag frequently have a huge spike
            in December.
            <br />
            Another somewhat visible trend in recent years is the spike of
            images with the maid and apron tags. This is probably caused by the
            so-called "Maid Day". This day originated from the reading of the
            Japanese word for maid (meido - メイド). In Japanese the start of
            this reading sounds similar to the English "may" and the "do" can
            stand for the 10th day of the month. On this day, cosplayers often
            post pictures in maid costumes or other things like animals are
            turned into maids.
          </p>
        </div>

        {this.renderTabsData(
          this.state.tableData_UniformsAndCostumes,
          this.state.pieOption_UniformsAndCostumes,
          this.state.yearChartData_UniformsAndCostumes,
          this.state.monthChartData_UniformsAndCostumes
        )}

        <BreakLine />

        <div>
          <h3>Swimsuits and Bodysuits</h3>
          <p>
            This tag group consists of different types of bodysuits. Swimsuits
            make up around 70% of all images in this group.
            <br />
            In the month view, you can see some repeating patterns. The swimsuit
            picks usually peak every year in August. The same pattern can also
            be found with the sarong tag, although on a smaller scale.
          </p>
        </div>

        {this.renderTabsData(
          this.state.tableData_SwimsuitsAndBodysuits,
          this.state.pieOption_SwimsuitsAndBodysuits,
          this.state.yearChartData_SwimsuitsAndBodysuits,
          this.state.monthChartData_SwimsuitsAndBodysuits
        )}

        <BreakLine />

        <div>
          <h3>Traditional Clothing</h3>
          <p>
            This tag group consists of different types of popular traditional
            clothing. As you can Japanese clothes (Kimonos, Hakamas, Mikos,
            Haori and Fundoshi) make up around 75% of all entries in this group.
            <br />
            In the month group you can see a repeating pattern that traditional
            clothes peak in January with the new year and in the summer where
            festivals take place and people also attend them in traditional
            clothes.
          </p>
        </div>

        {this.renderTabsData(
          this.state.tableData_TraditionalClothing,
          this.state.pieOption_TraditionalClothing,
          this.state.yearChartData_TraditionalClothing,
          this.state.monthChartData_TraditionalClothing
        )}
      </div>
    );
  }
}

export default ClothingPage;
