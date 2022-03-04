import React from "react";
import { withRouter } from "react-router-dom";
import { Row, Col, Button } from "react-bootstrap";
import { Route, Switch, Link } from "react-router-dom";
import Homepage from "../Homepage/Homepage";
import "./RoutingComponent.css";
import AutosuggestComponent from "../Autosuggest/AutosuggestComponent";

import CopyrightMainPage from "../CopyrightMainPage/CopyrightMainPage";
import CopyrightPage from "../CopyrightPage/CopyrightPage";

import CharacterMainPage from "../CharacterMainPage/CharacterMainPage";
import CharacterPage from "../CharacterPage/CharacterPage";

import TagMainPage from "../TagMainPage/TagMainPage";
import TagPage from "../TagPage/TagPage";
import ClothingPage from "../ClothingPage/ClothingPage";

/**
 * This component contains the react-router and is responsible for displaying different components based on the current route
 */
class RoutingComponent extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      value: "",
      suggestions: [],
      copyrightOption: "",
      characterOption: "",
      tagOption: "",
    };
  }

  /**
   * Setter method for the copyrightOption
   * @param {} option - new copyrightOption
   */
  setCopyrightOption = (option) => {
    this.setState({ copyrightOption: option });
  };

  /**
   * Setter method for the characterOption
   * @param {} option - new characterOption
   */
  setCharacterOption = (option) => {
    this.setState({ characterOption: option });
  };

  /**
   * Setter method for the tagOption
   * @param {} option - new tagOption
   */
  setTagOption = (option) => {
    this.setState({ tagOption: option });
  };

  /**
   * This method is used to go to a certain copyright route the route is based on the currently set copyrightOption
   */
  goToCopyright = () => {
    // The empty space is put before and after the option in order to cover edge cases such as options beginning or ending with ?
    this.props.history.push(
      "/copyright/" + " " + this.state.copyrightOption.replace("/", "%2F") + " "
    );
  };

  /**
   * This method is used to go to a certain character route the route is based on the currently set characterOption
   */
  goToCharacter = () => {
    // The empty space is put before and after the option in order to cover edge cases such as options beginning or ending with ?
    this.props.history.push(
      "/character/" + " " + this.state.characterOption.replace("/", "%2F") + " "
    );
  };

  /**
   * This method is used to go to a certain tag route the route is based on the currently set tagOption
   */
  goToTag = () => {
    // The empty space is put before and after the option in order to cover edge cases such as options beginning or ending with ?
    // The simple / within the option needs to be replaced by the url encoded version otherwise
    // it would be misinterpreted by the react router in the wrong way
    this.props.history.push(
      "/tag/" + " " + this.state.tagOption.replace("/", "%2F") + " "
    );

    console.log(this.state.tagOption);
  };

  render() {
    let loc = String(this.props.location.pathname);
    return (
      <div style={{ marginTop: "1%" }}>
        <Row>
          <Col className="col-2" style={{ marginLeft: "1%" }}>
            <div>
              <Row style={{ margin: "auto" }}>
                <h4 style={{ textDecorationLine: "underline" }}>Navigation</h4>
                <Link
                  style={{ color: "black", fontSize: "20px" }}
                  className={
                    (loc === "/" ? "headerlink-no-link " : "") +
                    "headerlink-title"
                  }
                  to="/"
                >
                  Home
                  <span
                    className={
                      loc === "/" ? "headerlink-dot-active" : "headerlink-dot"
                    }
                  >
                    •
                  </span>
                </Link>
              </Row>
              <Row style={{ margin: "auto" }}>
                <Link
                  style={{ color: "green", fontSize: "20px" }}
                  className={
                    (loc === "/character" ? "headerlink-no-link " : "") +
                    "headerlink-title"
                  }
                  to="/character"
                >
                  Character
                  <span
                    className={
                      loc === "/character"
                        ? "headerlink-dot-active"
                        : "headerlink-dot"
                    }
                  >
                    •
                  </span>
                </Link>

                {loc.startsWith("/character") ? (
                  <div>
                    <Row style={{ margin: "auto" }}>
                      <Link
                        style={{ color: "green", fontSize: "20px" }}
                        className={
                          (loc.startsWith("/character/hatsune_miku")
                            ? "headerlink-no-link "
                            : "") + "headerlink-title"
                        }
                        to="/character/hatsune_miku"
                      >
                        Hatsune Miku
                        <span
                          className={
                            loc.startsWith("/character/hatsune_miku")
                              ? "headerlink-dot-active"
                              : "headerlink-dot"
                          }
                        >
                          •
                        </span>
                      </Link>
                    </Row>
                    <Row style={{ margin: "auto" }}>
                      <Link
                        style={{ color: "green", fontSize: "20px" }}
                        className={
                          (loc.startsWith("/character/hakurei_reimu")
                            ? "headerlink-no-link "
                            : "") + "headerlink-title"
                        }
                        to="/character/hakurei_reimu"
                      >
                        Hakurei Reimu
                        <span
                          className={
                            loc.startsWith("/character/hakurei_reimu")
                              ? "headerlink-dot-active"
                              : "headerlink-dot"
                          }
                        >
                          •
                        </span>
                      </Link>
                    </Row>
                    <Row style={{ margin: "auto" }}>
                      <Link
                        style={{ color: "green", fontSize: "20px" }}
                        className={
                          (loc.startsWith("/character/admiral_(kancolle)")
                            ? "headerlink-no-link "
                            : "") + "headerlink-title"
                        }
                        to="/character/admiral_(kancolle)"
                      >
                        Admiral (KanColle)
                        <span
                          className={
                            loc.startsWith("/character/admiral_(kancolle)")
                              ? "headerlink-dot-active"
                              : "headerlink-dot"
                          }
                        >
                          •
                        </span>
                      </Link>
                    </Row>
                    <Row style={{ margin: "auto" }}>
                      <Link
                        style={{ color: "green", fontSize: "20px" }}
                        className={
                          (loc.startsWith("/character/artoria_pendragon_(fate)")
                            ? "headerlink-no-link "
                            : "") + "headerlink-title"
                        }
                        to="/character/artoria_pendragon_(fate)"
                      >
                        Artoria Pendragon (Fate)
                        <span
                          className={
                            loc.startsWith(
                              "/character/artoria_pendragon_(fate)"
                            )
                              ? "headerlink-dot-active"
                              : "headerlink-dot"
                          }
                        >
                          •
                        </span>
                      </Link>
                    </Row>
                    <Row style={{ margin: "auto" }}>
                      <h5 style={{ color: "green" }}>Go to Character:</h5>
                    </Row>
                    <Row style={{ margin: "auto" }}>
                      <Col className="col-8 my-auto d-flex justify-content-center">
                        <AutosuggestComponent
                          setOption={this.setCharacterOption}
                          suggestionList={this.props.characterOptions}
                        />
                      </Col>
                      <Col className="col-1" />
                      <Col className="col-2">
                        <div className="my-auto d-flex justify-content-center">
                          <Button
                            onClick={this.goToCharacter}
                            style={{
                              backgroundColor: "green",
                              border: "green",
                            }}
                          >
                            Go
                          </Button>
                        </div>
                      </Col>
                      <Col className="col-1" />
                    </Row>
                  </div>
                ) : null}
              </Row>
              <Row style={{ margin: "auto" }}>
                <Link
                  style={{ color: "purple", fontSize: "20px" }}
                  className={
                    (loc === "/copyright" ? "headerlink-no-link " : "") +
                    "headerlink-title"
                  }
                  to="/copyright"
                >
                  Copyright
                  <span
                    className={
                      loc === "/copyright"
                        ? "headerlink-dot-active"
                        : "headerlink-dot"
                    }
                  >
                    •
                  </span>
                </Link>
                {loc.startsWith("/copyright") ? (
                  <div>
                    <Row style={{ margin: "auto" }}>
                      <Link
                        style={{ color: "purple", fontSize: "20px" }}
                        className={
                          (loc.startsWith("/copyright/touhou")
                            ? "headerlink-no-link "
                            : "") + "headerlink-title"
                        }
                        to="/copyright/touhou"
                      >
                        Touhou
                        <span
                          className={
                            loc.startsWith("/copyright/touhou")
                              ? "headerlink-dot-active"
                              : "headerlink-dot"
                          }
                        >
                          •
                        </span>
                      </Link>
                    </Row>

                    <Row style={{ margin: "auto" }}>
                      <Link
                        style={{ color: "purple", fontSize: "20px" }}
                        className={
                          (loc.startsWith("/copyright/original")
                            ? "headerlink-no-link "
                            : "") + "headerlink-title"
                        }
                        to="/copyright/original"
                      >
                        Original
                        <span
                          className={
                            loc.startsWith("/copyright/original")
                              ? "headerlink-dot-active"
                              : "headerlink-dot"
                          }
                        >
                          •
                        </span>
                      </Link>
                    </Row>

                    <Row style={{ margin: "auto" }}>
                      <Link
                        style={{ color: "purple", fontSize: "20px" }}
                        className={
                          (loc.startsWith("/copyright/kantai_collection")
                            ? "headerlink-no-link "
                            : "") + "headerlink-title"
                        }
                        to="/copyright/kantai_collection"
                      >
                        Kantai Collection
                        <span
                          className={
                            loc.startsWith("/copyright/kantai_collection")
                              ? "headerlink-dot-active"
                              : "headerlink-dot"
                          }
                        >
                          •
                        </span>
                      </Link>
                    </Row>

                    <Row style={{ margin: "auto" }}>
                      <Link
                        style={{ color: "purple", fontSize: "20px" }}
                        className={
                          (loc.startsWith("/copyright/fate_(series)")
                            ? "headerlink-no-link "
                            : "") + "headerlink-title"
                        }
                        to="/copyright/fate_(series)"
                      >
                        Fate (Series)
                        <span
                          className={
                            loc.startsWith("/copyright/fate_(series)")
                              ? "headerlink-dot-active"
                              : "headerlink-dot"
                          }
                        >
                          •
                        </span>
                      </Link>
                    </Row>

                    <Row style={{ margin: "auto" }}>
                      <h5 style={{ color: "purple" }}>Go to Copyright:</h5>
                    </Row>
                    <Row style={{ margin: "auto" }}>
                      <Col className="col-8 my-auto d-flex justify-content-center">
                        <AutosuggestComponent
                          setOption={this.setCopyrightOption}
                          suggestionList={this.props.copyrightOptions}
                        />
                      </Col>
                      <Col className="col-1" />
                      <Col className="col-2">
                        <div className="my-auto d-flex justify-content-center">
                          <Button
                            onClick={this.goToCopyright}
                            style={{
                              backgroundColor: "purple",
                              border: "purple",
                            }}
                          >
                            Go
                          </Button>
                        </div>
                      </Col>
                      <Col className="col-1" />
                    </Row>
                  </div>
                ) : null}
              </Row>

              <Row style={{ margin: "auto" }}>
                <Link
                  style={{ color: "blue", fontSize: "20px" }}
                  className={
                    (loc === "/tag" ? "headerlink-no-link " : "") +
                    "headerlink-title"
                  }
                  to="/tag"
                >
                  Tags
                  <span
                    className={
                      loc === "/tag"
                        ? "headerlink-dot-active"
                        : "headerlink-dot"
                    }
                  >
                    •
                  </span>
                </Link>

                {loc.startsWith("/tag") ? (
                  <div>
                    <Row style={{ margin: "auto" }}>
                      <Link
                        style={{ color: "blue", fontSize: "20px" }}
                        className={
                          (loc.startsWith("/tag/clothing")
                            ? "headerlink-no-link "
                            : "") + "headerlink-title"
                        }
                        to="/tag/clothing"
                      >
                        Clothing
                        <span
                          className={
                            loc.startsWith("/tag/clothing")
                              ? "headerlink-dot-active"
                              : "headerlink-dot"
                          }
                        >
                          •
                        </span>
                      </Link>
                    </Row>
                    <Row style={{ margin: "auto" }}>
                      <h5 style={{ color: "blue" }}>Go to Tag:</h5>
                    </Row>
                    <Row style={{ margin: "auto" }}>
                      <Col className="col-8 my-auto d-flex justify-content-center">
                        <AutosuggestComponent
                          setOption={this.setTagOption}
                          suggestionList={this.props.generalTagOptions}
                        />
                      </Col>
                      <Col className="col-1" />
                      <Col className="col-2">
                        <div className="my-auto d-flex justify-content-center">
                          <Button
                            onClick={this.goToTag}
                            style={{
                              backgroundColor: "blue",
                              border: "blue",
                            }}
                          >
                            Go
                          </Button>
                        </div>
                      </Col>
                      <Col className="col-1" />
                    </Row>
                  </div>
                ) : null}
              </Row>
            </div>
          </Col>

          {/* Here the section for the react.-router with the routerswitsch starts */}

          <Col className="col-9">
            <Switch>
              <Route exact path="/character">
                <CharacterMainPage
                  refetchRoutes={this.props.refetchRoutes}
                  charList={this.props.charList}
                />
              </Route>
              <Route path="/character/:character">
                <CharacterPage
                  refetchRoutes={this.props.refetchRoutes}
                  charList={this.props.charList}
                />
              </Route>
              <Route exact path="/copyright">
                <CopyrightMainPage
                  refetchRoutes={this.props.refetchRoutes}
                  copyList={this.props.copyList}
                />
              </Route>

              <Route path="/copyright/:copyright">
                <CopyrightPage
                  refetchRoutes={this.props.refetchRoutes}
                  copyList={this.props.copyList}
                />
              </Route>

              <Route exact path="/tag">
                <TagMainPage
                  refetchRoutes={this.props.refetchRoutes}
                  tagList={this.props.tagList}
                />
              </Route>

              <Route exact path="/tag/clothing">
                <ClothingPage refetchRoutes={this.props.refetchRoutes} />
              </Route>

              <Route path="/tag/:tag">
                <TagPage
                  refetchRoutes={this.props.refetchRoutes}
                  tagList={this.props.tagList}
                />
              </Route>

              <Route path="/error">
                <h1>
                  The path you tried to access didnt exist. Please wait a moment
                  until the data for all possible routes is obtained and try it
                  again then. If you end again up on this page then the path you
                  are searching for doesnt exist.{" "}
                </h1>
              </Route>
              <Route path="/">
                <Homepage
                  charList={this.props.charList}
                  copyList={this.props.copyList}
                  tagList={this.props.tagList}
                />
              </Route>
            </Switch>
          </Col>
          <Col className="col-1"></Col>
        </Row>
      </div>
    );
  }
}
export default withRouter(RoutingComponent);
