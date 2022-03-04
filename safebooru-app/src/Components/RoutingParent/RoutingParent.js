import React from "react";

import axios from "axios";
import { BrowserRouter } from "react-router-dom";
import RoutingComponent from "../RoutingComponent/RoutingComponent";
/**
 * This component contains the methods for obtaining the lists used in the routing process
 * Those lists need to be given to the component for the routing from the outside
 */
class RoutingParent extends React.Component {
  constructor(props) {
    super(props);
    this.baseUrl = "http://safeana.f4.htw-berlin.de:8080/";
    //this.baseUrl = "http://localhost:8080/"
    this.state = {
      copyrightList: [],
      characterList: [],
      generalTagList: [],
      copyrightOptions: [],
      characterOptions: [],
      generalTagOptions: [],
    };
  }

  /**
   * This method obtains a list of all copyrights which is used for routing purposes
   * @returns the list of all copyrights
   */
  fetchCopyrightList = async () => {
    let copyrightListData = await (
      await axios.get(this.baseUrl + "copyright/list")
    ).data;

    return copyrightListData;
  };

  /**
   * This method obtains a list of all characters which is used for routing purposes
   * @returns the list of all characters
   */
  fetchCharacterList = async () => {
    let characterListData = await (
      await axios.get(this.baseUrl + "character/list")
    ).data;

    return characterListData;
  };

  /**
   * This method obtains a list of all generalTags which is used for routing purposes
   * @returns the list of all generalTags
   */
  fetchTagList = async () => {
    let tagListData = await (
      await axios.get(this.baseUrl + "generalTag/list")
    ).data;

    return tagListData;
  };

  /**
   * Method which is called once the component is mounted
   * In this method the lists that the routing is based on are obtained and set
   */
  componentDidMount = async () => {
    try {
      let [copyListData, charListData, tagListData] = await Promise.all([
        this.fetchCopyrightList(),
        this.fetchCharacterList(),
        this.fetchTagList(),
      ]);

      this.setState({
        copyrightList: copyListData,
        characterList: charListData,
        generalTagList: tagListData,
      });

      let copyOptions = copyListData.map((elem) => ({
        value: elem,
      }));

      let charOptions = charListData.map((elem) => ({
        value: elem,
      }));

      let tagOptions = tagListData.map((elem) => ({
        value: elem,
      }));

      this.setState({
        copyrightOptions: copyOptions,
        characterOptions: charOptions,
        generalTagOptions: tagOptions,
      });
    } catch (error) {
      console.log(error);
    }
  };

  /**
   * Method which is called each time the component structure updates
   * It checks if any of the routing lists has a length of 0
   * This would indicate that the given list isnt present/wasnt fetched
   * Thus the lists are reobtained via calling the componentDidMount() method
   */
  componentDidUpdate = async () => {
    if (
      this.state.copyrightList.length === 0 ||
      this.state.characterList.length === 0 ||
      this.state.generalTagList.length === 0
    ) {
      this.componentDidMount();
    }
  };

  /**
   * Wrapper method for the componentDidMount method that can be passed down to other components
   */
  refetchRoutes = async () => {
    this.componentDidMount();
  };

  render() {
    return (
      <div>
        <BrowserRouter>
          <RoutingComponent
            refetchRoutes={this.refetchRoutes}
            copyList={this.state.copyrightList}
            charList={this.state.characterList}
            tagList={this.state.generalTagList}
            copyrightOptions={this.state.copyrightOptions}
            characterOptions={this.state.characterOptions}
            generalTagOptions={this.state.generalTagOptions}
          />
        </BrowserRouter>
      </div>
    );
  }
}
export default RoutingParent;
