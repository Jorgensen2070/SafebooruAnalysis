import React from "react";
import Carousel from "react-bootstrap/Carousel";
import Image from "react-bootstrap/Image";
import { withRouter } from "react-router";
import DatafetchAnimation from "../Datafetchanimation/DatafetchAnimation";
/**
 * This component is the homepage/starting page currently it displays 5 selected iamges in a carousel and some text
 */
class Homepage extends React.Component {
  constructor(props) {
    super(props);
  }

  render() {
    return (
      <div>
        <div className="d-flex justify-content-center">
          <div>
            <h1 className="d-flex justify-content-center">
              Welcome to Safebooru-Analysis
            </h1>

            <p>
              This site is the react-application created as part of my bachelor
              thesis, which covered an analysis of the image metadata of the
              popular imageboard safebooru.
              <br />
              On this site, the data that was gathered and analyzed by other
              components is visualized and displayed in different graphs and
              tables.
              <br />
              There are 3 main groups of information that can be accessed -
              Characters, Copyrights, and Tags.
              <br />
              If you would like to access them, please click on the respective
              names in the navigation bar, which can be found on the left.
              <br />
              If you would like to search for a specific Character, Copyright,
              or Tag you first need to click on the respective section.
              Thereafter a search field appears where you can execute a search.
            </p>

            <div className="d-flex justify-content-center">
              <Carousel
                style={{ width: "1000px", height: "500px" }}
                variant="dark"
              >
                <Carousel.Item>
                  <Image
                    className="d-block mx-auto  my-auto"
                    style={{ height: "500px" }}
                    thumbnail
                    src="https://cdn.donmai.us/sample/da/cf/__eve_original_drawn_by_chihuri__sample-dacf5987ec7d099fef08ab2c2e626233.jpg"
                  />
                </Carousel.Item>
                <Carousel.Item>
                  <Image
                    className="d-block mx-auto  my-auto"
                    style={{ height: "500px" }}
                    thumbnail
                    src="https://cdn.donmai.us/original/21/59/__eve_original_drawn_by_chihuri__21599cd0cc617444a0238b0f3ec80431.jpg"
                  />
                </Carousel.Item>

                <Carousel.Item>
                  <Image
                    className="d-block mx-auto my-auto"
                    style={{ height: "500px" }}
                    thumbnail
                    src="https://cdn.donmai.us/original/91/09/__eve_original_drawn_by_chihuri__910975efe6bd29e98bf78fb7c1afbbc1.jpg"
                  />
                </Carousel.Item>

                <Carousel.Item>
                  <Image
                    className="d-block mx-auto  my-auto"
                    style={{ height: "500px" }}
                    thumbnail
                    src="https://cdn.donmai.us/original/d3/98/__eve_original_drawn_by_chihuri__d3985e966f9166686071f57e83cf2551.jpg"
                  />
                </Carousel.Item>
              </Carousel>
            </div>
            <p className="d-flex justify-content-center">
              Here you can see some example images gathered from Safebooru from
              the artist "Chihuri" displaying the character "zoya petrovna
              vecheslova".
            </p>

            <p>
              Please be aware that the data displayed on this site was gathered
              from safebooru and due to the nature of safebooru as a community
              driven platform some incorrect information or copyrighted images
              that were not flagged accordingly at the time of the data
              gathering may be present on this site.
            </p>

            <p>
              Please note that the component serving the data to the
              react-application is not available 24/7 due to certain
              restrictions. In the following field, you can see if the component
              is currently active. If the status does not change to "Service is
              active" after some time, this means that the react-application is
              unable to reach the component serving the data.
            </p>
            <div className="d-flex justify-content-center">
              <h4> Service Status - </h4>
              <h4> </h4>
              {this.props.charList.length > 0 &&
              this.props.copyList.length > 0 &&
              this.props.tagList.length > 0 ? (
                <h4>Service is active </h4>
              ) : (
                <h4> Trying to reach the service </h4>
              )}
            </div>
          </div>
        </div>
      </div>
    );
  }
}

export default withRouter(Homepage);
