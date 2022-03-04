import React from "react";
import "./AutosuggestComponent.css";

import Autosuggest from "react-autosuggest";

/**
 * This component returns an autosuggestComponent
 * This component receives 2 props:
 *
 * - setOption(option) - a method for setting the option for the site that will be accessed
 * - suggestionList - a list of possible suggestions
 */
class AutosuggestComponent extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      value: "",
      suggestions: [],
      display: false,
    };
  }

  /**
   * A function wich returns a list of possible suggestions depening on the given value
   *
   * @param {*} value - the value on which the suggestion should be based
   * @returns - the ist of possible suggestions
   */
  getSuggestions = (value) => {
    const inputValue = value.trim().toLowerCase();
    const inputLength = inputValue.length;

    return inputLength === 0
      ? []
      : this.props.suggestionList
          .filter(
            (lang) =>
              lang.value.toLowerCase().slice(0, inputLength) === inputValue
          )
          .slice(0, 10);
  };

  // When suggestion is clicked, Autosuggest needs to populate the input
  // based on the clicked suggestion. Teach Autosuggest how to calculate the
  // input value for every given suggestion.
  getSuggestionValue = (suggestion) => {
    return suggestion.value;
  };

  // Use your imagination to render suggestions.
  renderSuggestion = (suggestion) => {
    return <span>{suggestion.value}</span>;
  };

  onChange = (event, { newValue }) => {
    this.setState({
      value: newValue,
    });

    this.props.setOption(newValue);
  };

  // Autosuggest will call this function every time you need to update suggestions.
  // You already implemented this logic above, so just use it.
  onSuggestionsFetchRequested = ({ value }) => {
    this.setState({
      suggestions: this.getSuggestions(value),
    });
  };

  // Autosuggest will call this function every time you need to clear suggestions.
  onSuggestionsClearRequested = () => {
    this.setState({
      suggestions: [],
    });
  };

  render() {
    const { value, suggestions } = this.state;
    const inputProps = {
      placeholder: "Search",
      value,
      onChange: this.onChange,
    };

    return (
      <Autosuggest
        suggestions={suggestions}
        onSuggestionsFetchRequested={this.onSuggestionsFetchRequested}
        onSuggestionsClearRequested={this.onSuggestionsClearRequested}
        getSuggestionValue={this.getSuggestionValue}
        renderSuggestion={this.renderSuggestion}
        inputProps={inputProps}
      />
    );
  }
}

export default AutosuggestComponent;
