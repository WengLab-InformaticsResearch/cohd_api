const baseURL = new URL('https://covid.cohd.io')

const exampleInputs1 = [
    {inner: '<span class="information">Example OMOP concept IDs:</span>', value: ''},
    {inner: 'Atrial fibrillation <span class="information">(313217)</span>', value: 313217},
    {inner: 'Cancer in situ of urinary bladder <span class="information">(192855)</span>', value: 192855},
    {inner: 'Situs inversus viscerum <span class="information">(193306)</span>', value: 193306},
    {inner: 'Ibuprofen 600 MG Oral Tablet <span class="information">(19019073)</span>', value: 19019073},
    {inner: 'atorvastatin 20 MG Oral Tablet <span class="information">(19123592)</span>', value: 19123592},
    {inner: 'Albuterol 0.83 MG/ML Inhalant Solution <span class="information">(19123989)</span>', value: 19123989},
    {inner: 'Laparoscopy, surgical, appendectomy <span class="information">(2109144)</span>', value: 2109144},
    {inner: 'Magnetic resonance imaging of brain and brain stem <span class="information">(2006956)</span>', value: 2006956},
    {inner: 'Closed [endoscopic] biopsy of large intestine <span class="information">(2002705)</span>', value: 2002705}
  ];

const exampleInputs2 = [
    {inner: '<span class="information">Example OMOP concept IDs:</span>', value: ''},
    {inner: 'Essential hypertension <span class="information">(320128)</span>', value: 320128},
    {inner: 'Osteoarthritis <span class="information">(80180)</span>', value: 80180},
    {inner: '1000 ML Sodium Chloride 9 MG/ML Injection <span class="information">(40220357)</span>', value: 40220357},
    {inner: 'Benzocaine 200 MG/ML Topical Spray <span class="information">(19099840)</span>', value: 19099840},
    {inner: 'Adult health examination <span class="information">(4145333)</span>', value: 4145333},
    {inner: 'Collection of venous blood by venipuncture <span class="information">(2108115)</span>', value: 2108115}
  ];

const setDomains = new Set(["condition", "device", "drug", "ethnicity", "gender", "measurement", "observation", "procedure", "race"]);

const setConceptClasses = new Set(["ingredient"]);

const messageConcept1 = "For Concept 1, please either: 1) enter an OMOP concept ID; or 2) search for a concept by name and choose a concept ID from the list of suggestions.";

const messageConcept2 = "For Concept 2, please either: 1) enter an OMOP concept ID; 2) search for a concept by name and choose a concept ID from the list of suggestions; 3) enter a domain; or 4) leave the field blank to retrieve associations with all concepts.";

const blockUIOptions = {
    message: '<h2>Retrieving COHD data. Please wait...</h2>',
    css: {backgroundColor: '#ddd', color: '#00539F'},
    fadeOut: 200,
    timeout: 60000,
    focusInput: false
    };

/*********************************************************************************
* Autocomplete
*********************************************************************************/
// Delays calls to the callback
function delay(callback, ms) {
  var timer = 0;
  return function() {
    var context = this, args = arguments;
    clearTimeout(timer);
    timer = setTimeout(function () {
      callback.apply(context, args);
    }, ms || 0);
  };
}

// Show autocomplete suggestions based on search matches
function conceptAutocomplete(e, inp) {
  // If user presses Esc, close all lists
  if (e.keyCode == 27) {
    closeAutocompleteLists();
    return;
  }

  // Ignore keys that don't change the input
  // Allow down arrow to trigger autocomplete if the list isn't open
  if ((e.keyCode >= 9 && e.keyCode <= 45 && e.keyCode != 32 && e.keyCode != 40) ||
      (e.keyCode >= 91 && e.keyCode <= 93) ||
      (e.keyCode >= 112 && e.keyCode <= 145) ||
      (e.keyCode == 40 && document.getElementById(inp.id + "autocomplete-list"))) {
    return;
  }

  var val = inp.value;

  // InputConcept1: if the input is empty, show a list of example concepts.
  // If the input has 1-2 characters, don't perform the search
  if (inp.id == "inputConcept1") {
    if (val.length == 0) {
      // Show example inputs for Concept 1
      updateAutocompleteList(inp, exampleInputs1);
      return;
    } else if ((val.length > 0 && val.length < 3) ||   // Input is less than 3 characters
        checkConceptID(val)) {                        // Input looks like a concept ID
      closeAutocompleteLists();
      return;
    }
  }

  // InputConcept2: if the input is empty, show a list of domains.
  // If the input has 1-2 characters, don't perform the search
  if (inp.id == "inputConcept2") {
    if (val.length == 0) {
      // Suggest domains when there is nothing typed into Concept 2
      // Retrieve domains from domainCounts endpoint
      var datasetID = $("#comboDataset").val();
      var url = new URL("/api/metadata/domainCounts?", baseURL);
      url += encodeQueryData({dataset_id: datasetID});
      $.get(url, function(data, status) {
        var arr = [{inner: '<span class="information">Domains:</span>', value: ''}];
        var domainLabel = '<span class="information"> (domain)</span>';
        if (data && data.hasOwnProperty("results")) {
          arr = arr.concat(data.results.map(function (x) {
            return {inner: x.domain_id + domainLabel, value: x.domain_id};
          }));
        } else {
          // No results found, use the default set of domains
          arr = arr.concat(Array.from(setDomains).map(function (domain) {
            return {inner: domain + domainLabel, value: domain};
          }));
        }
        arr = arr.concat(exampleInputs2);
        updateAutocompleteList(inp, arr);
      });
      return;
    } else if ((val.length > 0 && val.length < 3) ||    // Input is 1-2 charset
        checkConceptID(val) ||                          // Input looks like a concept ID
        setDomains.has(val.toLowerCase()) ||            // Input is a domain
        setConceptClasses.has(val.toLowerCase()))       // Input is a concept class
    {
      closeAutocompleteLists();
      return;
    }
  }

  // Update the autocomplete list with search results from COHD
  var url = new URL("/api/omop/findConceptIDs?", baseURL);
  var datasetID = $("#comboDataset").val();
  url += encodeQueryData({q: inp.value, dataset_id: datasetID, min_count: 1});
  $.get(url, function(data, status) {
    if (data && data.hasOwnProperty("results")) {
      var arr = data["results"].map(function (concept) {
        /*create a DIV element for each matching element:*/
        var inner = concept["concept_name"] +
          '<span class="information">' + " (count: " + concept["concept_count"].toLocaleString() +
          "; ID: " + concept["concept_id"] + ")</span>";
        /*insert a input field that will hold the current array item's value:*/
        return {inner: inner, value: concept["concept_id"]};
      });

      updateAutocompleteList(inp, arr);
    }
  });
}

// Update the autocomplete options
function updateAutocompleteList(inp, arr) {
  var maxListLength = 15;

  var a, b, i;

  /*close any already open lists of autocompleted values*/
  closeAutocompleteLists();

  /*create a DIV element that will contain the items (values):*/
  a = document.createElement("DIV");
  a.setAttribute("id", inp.id + "autocomplete-list");
  a.setAttribute("class", "autocomplete-items");
  /*append the DIV element as a child of the autocomplete container:*/
  inp.parentNode.appendChild(a);

  /*for each item in the array...*/
  for (i = 0; i < arr.length; i++) {
    if (i >= maxListLength) {
      // Don't add any more items
      break;
    }

    /*create a DIV element for each matching element:*/
    b = document.createElement("DIV");
    /*make the matching letters bold:*/
    b.innerHTML = arr[i]["inner"];
    /*insert a input field that will hold the current array item's value:*/
    b.innerHTML += "<input type='hidden' value='" + arr[i]["value"] + "'>";
    /*execute a function when someone clicks on the item value (DIV element):*/
    b.addEventListener("click", function(e) {
      /*insert the value for the autocomplete text field:*/
      inp.value = this.getElementsByTagName("input")[0].value;
      /*close the list of autocompleted values,
      (or any other open lists of autocompleted values:*/
      closeAutocompleteLists();
    });
    a.appendChild(b);
  }
}

/* Autocomplete */
function autocomplete(inp) {
  /*the autocomplete function takes two arguments,
  the text field element and an array of possible autocompleted values:*/
  var currentFocus;

  /*execute a function when someone writes in the text field:*/
  currentFocus = -1;

  /*execute a function presses a key on the keyboard:*/
  inp.addEventListener("keydown", function(e) {
    var x = document.getElementById(this.id + "autocomplete-list");
    if (x) {
      x = x.getElementsByTagName("div");
      if (x.length > 0) {
        if (e.keyCode == 40) {
          /*If the arrow DOWN key is pressed,
          increase the currentFocus variable:*/
          currentFocus = (currentFocus + 1) % x.length;
          /*and and make the current item more visible:*/
          addActive(x);
        } else if (e.keyCode == 38) { //up
          /*If the arrow UP key is pressed,
          decrease the currentFocus variable:*/
          currentFocus = (currentFocus - 1) % x.length;
          /*and and make the current item more visible:*/
          addActive(x);
        } else if (e.keyCode == 13) {
          /*If the ENTER key is pressed, prevent the form from being submitted,*/
          e.preventDefault();
          if (currentFocus > -1 && currentFocus < x.length) {
            /*and simulate a click on the "active" item:*/
            if (x) x[currentFocus].click();
          }
        }
      }
    }
  });

  function addActive(x) {
    /*a function to classify an item as "active":*/
    if (!x || x.length == 0) return false;
    /*start by removing the "active" class on all items:*/
    removeActive(x);
    if (currentFocus >= x.length) currentFocus = 0;
    if (currentFocus < 0) currentFocus = (x.length - 1);
    /*add class "autocomplete-active":*/
    x[currentFocus].classList.add("autocomplete-active");
  }

  function removeActive(x) {
    /*a function to remove the "active" class from all autocomplete items:*/
    for (var i = 0; i < x.length; i++) {
      x[i].classList.remove("autocomplete-active");
    }
  }
}

function closeAutocompleteLists(elmnt) {
  /*close all autocomplete lists in the document,
  except the one passed as an argument:*/
  var x = document.getElementsByClassName("autocomplete-items");
  for (var i = 0; i < x.length; i++) {
    if (!elmnt || (elmnt != x[i] && !x[i].parentNode.contains(elmnt))) {
      x[i].parentNode.removeChild(x[i]);
    }
  }
}

// When inputConcept1 initially receives focus, show example concept IDs
function handleFocusConcept1(e) {
  conceptAutocomplete(e, e.target);
}

function handleFocusConcept2(e) {
  conceptAutocomplete(e, e.target);
}

/*********************************************************************************
* Display results
*********************************************************************************/
// Last COHD data table endpoint called
var lastURL;

function checkConceptID(str) {
  // Check if the input string is a valid concept ID by simply checking if it's all numeric characters
  return /^\d+$/.test(str)
}

function encodeQueryData(data) {
  // Convert dictionary into query parameters
  const ret = [];
  for (let d in data)
    ret.push(encodeURIComponent(d) + '=' + encodeURIComponent(data[d]));
  return ret.join('&');
}

function populateDatasetSelector() {
  // Add a static list of datasets by default
  var selDataset = $("#comboDataset");
  // selDataset.append('<option value="1" title="Clinical data from 2013-2017. Each concept\'s count reflects the use of that specific concept.">5-year non-hierarchical</option>')
  //  .append('<option value="2" title="Clinical data from all years in the database. Each concept\'s count reflects the use of that specific concept.">Lifetime non-hierarchical</option>')
  //  .append('<option value="3" title="Clinical data from 2013-2017. Each concept\'s count includes use of that concept and descendant concepts.">5-year hierarchical</option>');

  // Try to add list of datasets returned from 'datasets' endpoint
  var urlDataset = new URL("/api/metadata/datasets", baseURL);
  $.get(urlDataset, function(data, status) {
    if (data.hasOwnProperty("results") && data["results"].length >= 1) {
      var results = data["results"];
      var selDataset = $("#comboDataset");

      // Empty out the datasets added earlier by default
      selDataset.empty();

      // Add the retrieved list of datasets
      for (var i = 0; i < results.length; i++) {
        var result = results[i];
        if (result.hasOwnProperty("dataset_id") &&
          result.hasOwnProperty("dataset_name") &&
          result.hasOwnProperty("dataset_description")) {
            var newOption = $("<option/>", {
              value: result["dataset_id"],
              title: result["dataset_description"]
            });
            newOption.text(result["dataset_name"])
            if (i == results.length - 1) {
              newOption.attr("selected", "selected");
            }
            newOption.appendTo(selDataset);
        }
      }
    }
  });
}

function displayConcept1(data, status) {
  // Check if this concept has data
  if (data["results"].length > 0) {
    // Found the concept. Display concept definition.
    var concept = data["results"][0];
    $("#divConcept1Results").empty();
    $("#divConcept1Results").append("<p class=\"results\"><b>Concept ID: </b>" + concept["concept_id"] + "</p>")
      .append("<p class=\"results\"><b>Concept name:</b> " + concept["concept_name"] + "</p>")
      .append("<p class=\"results\"><b>Vocabulary:</b> " + concept["vocabulary_id"] + "</p>")
      .append("<p class=\"results\"><b>Concept code:</b> " + concept["concept_code"] + "</p>")
      .append("<p class=\"results\"><b>Domain:</b> " + concept["domain_id"] + "</p>")
      .append("<p class=\"results\"><b>Concept class:</b> " + concept["concept_class_id"] + "</p>");

    // Get single concept frequency

    var urlSingleConceptFreq = new URL("/api/frequencies/singleConceptFreq?", baseURL);
    urlSingleConceptFreq += encodeQueryData({
      dataset_id: $("#comboDataset").val(),
      q: concept["concept_id"]
    });
    $.get(urlSingleConceptFreq, function(data, status) {
      if (data["results"].length > 0) {
        var conceptData = data["results"][0];
        $("#divConcept1Results").append("<p class=\"results\"><b>Visit count:</b> " + conceptData["concept_count"] + "</p>")
          .append("<p class=\"results\"><b>Visit prevalence:</b> " + (conceptData["concept_frequency"] * 100).toFixed(6) + "%</p>");
      } else {
        $("#divConcept1Results").append("<p class=\"results\"><b>Visit count:</b> 0</p>")
          .append("<p class=\"results\"><b>Visit prevalence:</b> 0%</p>");
      }
    });
  } else {
    // No concept found.
    $("#divConcept1Results").html("<p class=\"results\"><b>Concept not found.</b></p>");
  }
}

function displayConcept2(data, status) {
  // Check if this concept has data
  if (data["results"].length > 0) {
    // Found the concept. Display concept definition.
    var concept = data["results"][0];
    $("#divConcept2Results").empty();
    $("#divConcept2Results").append("<p class=\"results\"><b>Concept ID: </b>" + concept["concept_id"] + "</p>")
      .append("<p class=\"results\"><b>Concept name:</b> " + concept["concept_name"] + "</p>")
      .append("<p class=\"results\"><b>Vocabulary:</b> " + concept["vocabulary_id"] + "</p>")
      .append("<p class=\"results\"><b>Concept code:</b> " + concept["concept_code"] + "</p>")
      .append("<p class=\"results\"><b>Domain:</b> " + concept["domain_id"] + "</p>")
      .append("<p class=\"results\"><b>Concept class:</b> " + concept["concept_class_id"] + "</p>");

    // Get single concept frequency
    var urlSingleConceptFreq = new URL("/api/frequencies/singleConceptFreq?", baseURL);
    urlSingleConceptFreq += encodeQueryData({
      dataset_id: $("#comboDataset").val(),
      q: concept["concept_id"]
    });
    $.get(urlSingleConceptFreq, function(data, status) {
      if (data["results"].length > 0) {
        var conceptData = data["results"][0];
        $("#divConcept2Results").append("<p class=\"results\"><b>Visit count:</b> " + conceptData["concept_count"] + "</p>")
          .append("<p class=\"results\"><b>Visit prevalence:</b> " + (conceptData["concept_frequency"] * 100).toFixed(6) + "%</p>");
      } else {
        $("#divConcept2Results").append("<p class=\"results\"><b>Visit count:</b> 0</p>")
          .append("<p class=\"results\"><b>Visit prevalence:</b> 0%</p>");
      }
    });
  } else {
    // No concept found.
    $("#divConcept2Results").html("<p class=\"results\"><b>Concept not found.</b></p>");
  }
}

function createCSVLink(results) {
  // Write results to CSV
  let csvContent = "data:text/csv;charset=utf-8,";
  csvContent += Object.keys(results[0]).join(",") + "\r\n";
  for(var i = 0; i < results.length; i++) {
   csvContent += Object.values(results[i]).map(function (x) {return '"' + x + '"'}).join(",") + "\r\n";
  }
  var encodedUri = encodeURI(csvContent);

  // Create the CSV link
  var link = document.createElement("a");
  link.setAttribute("href", encodedUri);
  link.setAttribute("download", "my_data.csv");
  link.text = "CSV";
  return link;
}

function displayDynatable(data, columns, fields) {
  // Check if this concept has data
  if (data["results"].length > 0) {
    // Received association data. Display in table
    var headers = [];
    for (var i = 0; i < columns.length; i++) {
      headers.push('<th data-dynatable-column="' + fields[i] + '">' + columns[i] + '</th>');
    }
    headers = headers.join("");
    $("#divPairedResults").empty();
    var dynatable = $("#divPairedResults").append('<table id="tablePairedResults"><thead>' + headers + '</thead><tbody></tbody></table>')
    try {
      $("#tablePairedResults").dynatable({
        dataset: {
          records: data["results"]
        }
      });
    } catch(err) {
      console.log(err);
    }
    dynatable.append('download: <a href="' + lastURL + '" target="_blank">JSON</a> | ');
    dynatable.append(createCSVLink(data["results"]));
  } else {
    // No associated data found.
    $("#divPairedResults").html("<p class=\"resultsPair\"><b>Associated data not found.</b></p>");
  }
}

function displayPairedCounts(data, status) {
  // Check if this concept has data
  if (data["results"].length == 1) {
    // Received data for a single pair of concepts. Display as text
    var pairedData = data["results"][0];
    $("#divPairedResults").empty()
      .append("<p class=\"resultsPair\"><b><u>Co-occurrence Count</u></b></p>")
      .append("<p class=\"resultsPair\"><b>Visit count:</b> " + pairedData["concept_count"] + "</p>")
      .append("<p class=\"resultsPair\"><b>Visit prevalence:</b> " + (pairedData["concept_frequency"] * 100).toFixed(6) + "%</p>");
  } else {
    // No associated data found.
    $("#divPairedResults").html("<p class=\"resultsPair\"><b>Associated data not found.</b></p><p class=\"resultsPair\">Fewer than 10 visits with this pair of concepts.</p>");
  }
}

function pairedCounts() {
  // Get concept ID 1 to look up
  var conceptID = $("#inputConcept1").val().trim();

  // The second input could be either a concept ID or domain
  var input2 = $("#inputConcept2").val().trim();

  // Get the chosen dataset ID
  var datasetID = $("#comboDataset").val().trim();

  // Clear out the division for a single concept2 results

  function displayAssociatedCounts(data, status) {
    // Multiply freq by 100 to display as %
    if (data.hasOwnProperty("results")) {
      for (var i = 0; i < data["results"].length; i++) {
        data["results"][i]["concept_frequency"] = (data["results"][i]["concept_frequency"] * 100).toFixed(6);
      }
    }

    var columns = ["Concept ID 2", "Concept 2 Name", "Concept 2 Domain", "Concept Pair Count", "Concept Pair Prevalence (%)"];
    var fields = ["associated_concept_id", "associated_concept_name", "associated_domain_id", "concept_count", "concept_frequency"];
    displayDynatable(data, columns, fields);

    // Allow user interaction again
    $.unblockUI();
  }

  if (input2.length == 0) {
    // No input specified for concept 2. Get co-occurrence counts between concept 1 and all concepts.
    var urlEndpoint = new URL("/api/frequencies/associatedConceptFreq?", baseURL);
    var queryParams = {"q": conceptID, "dataset_id": datasetID};
    var url = urlEndpoint + encodeQueryData(queryParams);
    $.get(url, displayAssociatedCounts);
    lastURL = url;

    // Prevent user interaction with site while loading association data
    $.blockUI(blockUIOptions);
  } else if(setDomains.has(input2.toLowerCase())) {
    // Domain specified for concept 2. Get co-occurrence counts between concept 1 and all concepts in domain.
    var urlEndpoint = new URL("/api/frequencies/associatedConceptDomainFreq?", baseURL);
    var queryParams = {"concept_id": conceptID, "domain": input2, "dataset_id": datasetID};
    var url = urlEndpoint + encodeQueryData(queryParams);
    $.get(url, displayAssociatedCounts);
    lastURL = url;

    // Prevent user interaction with site while loading association data
    $.blockUI(blockUIOptions);
  } else if(setConceptClasses.has(input2.toLowerCase())) {
    // Concept class specified for concept 2. Get co-occurrence counts between concept 1 and all concepts in class.
    var urlEndpoint = new URL("/api/frequencies/associatedConceptDomainFreq?", baseURL);
    var queryParams = {"concept_id": conceptID, "concept_class": input2, "dataset_id": datasetID};
    var url = urlEndpoint + encodeQueryData(queryParams);
    $.get(url, displayAssociatedCounts);
    lastURL = url;

    // Prevent user interaction with site while loading association data
    $.blockUI(blockUIOptions);
  } else if(checkConceptID(input2)) {
    // Show single concept info for concept 2
    // Call concepts endpoint to get information about the concept
    var urlConcept = new URL("/api/omop/concepts?", baseURL);
    urlConcept += encodeQueryData({
      q: input2,
      dataset_id: datasetID
    });
    $.get(urlConcept, displayConcept2);

    // Get co-occurrence counts between just concept 1 and concept 2
    var urlEndpoint = new URL("/api/frequencies/pairedConceptFreq?", baseURL);
    var queryParams = {"q": [conceptID, input2].join(","), "dataset_id": datasetID};
    var url = urlEndpoint + encodeQueryData(queryParams);
    $.get(url, displayPairedCounts);
  } else {
    $("#divPairedResults").empty();
    message(messageConcept2);
  }
}

function formatPValue(p) {
  var pStr;
  if (p < 0.0001) {
    pStr = p.toExponential(6);
  } else {
    pStr = p.toFixed(6);
  }
  return pStr;
}

function displayPairedChi(data, status) {
  // Check if this concept has data
  if (data["results"].length == 1) {
    // Received data for a single pair of concepts. Display as text
    var pairedData = data["results"][0];
    var pStr = formatPValue(pairedData["p-value"]);
    $("#divPairedResults").empty()
      .append("<p class=\"resultsPair\"><b><u>Chi-square</u></b></p>")
      .append("<p class=\"resultsPair\"><b>Chi-square:</b> " + pairedData["chi_square"].toFixed(6) + "</p>")
      .append("<p class=\"resultsPair\"><b>P-value:</b> " + pStr + "</p>")
      .append("<table><tr><td colspan='2' rowspan='2'></td><td colspan='3'>" + pairedData["concept_id_2"] + "</td></tr>" +
        "<tr><td>0</td><td>1</td><td>Total</td></tr>" +
        "<tr><td rowspan='3'>" + pairedData["concept_id_1"] + "</td><td>0</td><td>" + pairedData["n_~c1_~c2"] + "</td><td>" + pairedData["n_~c1_c2"] + "</td><td>" + (pairedData["n"] - pairedData["n_c1"]) + "</td></tr>" +
        "<tr><td>1</td><td>" + pairedData["n_c1_~c2"] + "</td><td>" + pairedData["n_c1_c2"] + "</td><td>" + pairedData["n_c1"] + "</td></tr>" +
        "<tr><td>Total</td><td>" + (pairedData["n"] - pairedData["n_c2"]) + "</td><td>" + pairedData["n_c2"] + "</td><td>" + pairedData["n"] + "</td></tr></table>");
  } else {
    // No associated data found.
    $("#divPairedResults").html("<p class=\"resultsPair\"><b>Associated data not found.</b></p><p class=\"resultsPair\">Fewer than 10 visits with this pair of concepts.</p>");
  }
}

function pairedChi() {
  var urlEndpoint = new URL("/api/association/chiSquare?", baseURL);

  // Get concept ID 1 to look up
  var conceptID = $("#inputConcept1").val().trim();

  // The second input could be either a concept ID or domain
  var input2 = $("#inputConcept2").val().trim();

  // Get the chosen dataset ID
  var datasetID = $("#comboDataset").val().trim();

  function displayAssociatedChi(data, status) {
    // Format the numbers for easier reading
    if (data.hasOwnProperty("results")) {
      for (var i = 0; i < data["results"].length; i++) {
        data["results"][i]["chi_square"] = data["results"][i]["chi_square"].toFixed(4);
        data["results"][i]["p-value"] = formatPValue(data["results"][i]["p-value"]);
      }
    }

    var columns = ["Concept ID 2", "Concept 2 Name", "Concept 2 Domain", "Chi-Square", "P-Value"];
    var fields = ["concept_id_2", "concept_2_name", "concept_2_domain", "chi_square", "p-value"];
    displayDynatable(data, columns, fields);

    // Allow user interaction again
    $.unblockUI();
  }

  if (input2.length == 0) {
    // No input specified for concept 2. Get chi-square between concept 1 and all concepts.      .
    var queryParams = {"concept_id_1": conceptID, "dataset_id": datasetID};
    var url = urlEndpoint + encodeQueryData(queryParams);
    $.get(url, displayAssociatedChi);
    lastURL = url;

    // Prevent user interaction with site while loading association data
    $.blockUI(blockUIOptions);
  } else if(setDomains.has(input2.toLowerCase())) {
    // Domain specified for concept 2. Get chi-square between concept 1 and all concepts in domain.
    var queryParams = {"concept_id_1": conceptID, "domain": input2, "dataset_id": datasetID};
    var url = urlEndpoint + encodeQueryData(queryParams);
    $.get(url, displayAssociatedChi);
    lastURL = url;

    // Prevent user interaction with site while loading association data
    $.blockUI(blockUIOptions);
  } else if(setConceptClasses.has(input2.toLowerCase())) {
    // Concept class specified for concept 2. Get chi-square between concept 1 and all concepts in class.
    var queryParams = {"concept_id_1": conceptID, "concept_class": input2, "dataset_id": datasetID};
    var url = urlEndpoint + encodeQueryData(queryParams);
    $.get(url, displayAssociatedChi);
    lastURL = url;

    // Prevent user interaction with site while loading association data
    $.blockUI(blockUIOptions);
  } else if(checkConceptID(input2)) {
    // Show single concept info for concept 2
    var urlConcept = new URL("/api/omop/concepts?", baseURL);
    urlConcept += encodeQueryData({
      q: input2,
      dataset_id: datasetID
    });
    $.get(urlConcept, displayConcept2);

    // Get chi-square between just concept 1 and concept 2
    var queryParams = {"concept_id_1": conceptID, "concept_id_2": input2, "dataset_id": datasetID};
    var url = urlEndpoint + encodeQueryData(queryParams);
    $.get(url, displayPairedChi);
  } else {
    $("#divPairedResults").empty();
    message(messageConcept2);
  }
}

function displayPairedOEFR(data, status) {
  // Check if this concept has data
  if (data["results"].length == 1) {
    // Received data for a single pair of concepts. Display as text
    var pairedData = data["results"][0];
    $("#divPairedResults").empty()
      .append("<p class=\"resultsPair\"><b><u>Observed-to-expected frequency ratio</u></b></p>")
      .append("<p class=\"resultsPair\"><b>Observed count: </b> " + pairedData["observed_count"] + "</p>")
      .append("<p class=\"resultsPair\"><b>Expected count: </b> " + pairedData["expected_count"].toFixed(6) + "</p>")
      .append("<p class=\"resultsPair\"><b>Log ratio: </b> " + pairedData["ln_ratio"] + "</p>")
      .append("<p class=\"resultsPair\"><b>Log ratio confidence interval: </b> " + pairedData["confidence_interval"] + "</p>");
  } else {
    // No associated data found.
    $("#divPairedResults").html("<p class=\"resultsPair\"><b>Associated data not found.</b></p><p class=\"resultsPair\">Fewer than 10 visits with this pair of concepts.</p>");
  }
}

function pairedOEFR() {
  var urlEndpoint = new URL("/api/association/obsExpRatio?", baseURL);

  // Get concept ID 1 to look up
  var conceptID = $("#inputConcept1").val().trim();

  // The second input could be either a concept ID or domain
  var input2 = $("#inputConcept2").val().trim();

  // Get the chosen dataset ID
  var datasetID = $("#comboDataset").val().trim();

  function displayAssociatedOEFR(data, status) {
    // Format the numbers for easier reading
    if (data.hasOwnProperty("results")) {
      for (var i = 0; i < data["results"].length; i++) {
        data["results"][i]["expected_count"] = data["results"][i]["expected_count"].toFixed(6);
        data["results"][i]["ln_ratio"] = data["results"][i]["ln_ratio"].toFixed(6);
        ci = data["results"][i]["confidence_interval"];
        data["results"][i]["confidence_interval"] = [ci[0].toFixed(6), ci[1].toFixed(6)];
      }
    }

    var columns = ["Concept ID 2", "Concept 2 Name", "Concept 2 Domain", "Observed Count", "Expected Count", "Log Ratio", "Confidence Interval"];
    var fields = ["concept_id_2", "concept_2_name", "concept_2_domain", "observed_count", "expected_count", "ln_ratio", "confidence_interval"];
    displayDynatable(data, columns, fields);

    // Allow user interaction again
    $.unblockUI();
  }

  if (input2.length == 0) {
    // No input specified for concept 2. Get OEFR between concept 1 and all concepts.      .
    var queryParams = {"concept_id_1": conceptID, "dataset_id": datasetID};
    var url = urlEndpoint + encodeQueryData(queryParams);
    $.get(url, displayAssociatedOEFR);
    lastURL = url;

    // Prevent user interaction with site while loading association data
    $.blockUI(blockUIOptions);
  } else if(setDomains.has(input2.toLowerCase())) {
    // Domain specified for concept 2. Get OEFR between concept 1 and all concepts in domain.
    var queryParams = {"concept_id_1": conceptID, "domain": input2, "dataset_id": datasetID};
    var url = urlEndpoint + encodeQueryData(queryParams);
    $.get(url, displayAssociatedOEFR);
    lastURL = url;

    // Prevent user interaction with site while loading association data
    $.blockUI(blockUIOptions);
  } else if(setConceptClasses.has(input2.toLowerCase())) {
    // Concept class specified for concept 2. Get OEFR between concept 1 and all concepts in class.
    var queryParams = {"concept_id_1": conceptID, "concept_class": input2, "dataset_id": datasetID};
    var url = urlEndpoint + encodeQueryData(queryParams);
    $.get(url, displayAssociatedOEFR);
    lastURL = url;

    // Prevent user interaction with site while loading association data
    $.blockUI(blockUIOptions);
  } else if(checkConceptID(input2)) {
    // Show single concept info for concept 2
    var urlConcept = new URL("/api/omop/concepts?", baseURL);
    urlConcept += encodeQueryData({
      q: input2,
      dataset_id: datasetID
    });
    $.get(urlConcept, displayConcept2);

    // Get OEFR between just concept 1 and concept 2
    var queryParams = {"concept_id_1": conceptID, "concept_id_2": input2, "dataset_id": datasetID};
    var url = urlEndpoint + encodeQueryData(queryParams);
    $.get(url, displayPairedOEFR);
  } else {
    $("#divPairedResults").empty();
    message(messageConcept2);
  }
}

function displayPairedRF(data, status) {
  // Check if this concept has data
  if (data["results"].length == 1) {
    // Received data for a single pair of concepts. Display as text
    var pairedData = data["results"][0];
    $("#divPairedResults").empty()
      .append("<p class=\"resultsPair\"><b><u>Relative frequency</u></b></p>")
      .append("<p class=\"resultsPair\"><b>Co-occurrence count: </b> " + pairedData["concept_pair_count"] + "</p>")
      .append("<p class=\"resultsPair\"><b>Relative frequency: </b> " + (pairedData["relative_frequency"] * 100).toFixed(6) + "%</p>");
  } else {
    // No associated data found.
    $("#divPairedResults").html("<p class=\"resultsPair\"><b>Associated data not found.</b></p><p class=\"resultsPair\">Fewer than 10 visits with this pair of concepts.</p>");
  }
}

function pairedRF() {
  var urlEndpoint = new URL("/api/association/relativeFrequency?", baseURL);

  // Get concept ID 1 to look up
  var conceptID = $("#inputConcept1").val().trim();

  // The second input could be either a concept ID or domain
  var input2 = $("#inputConcept2").val().trim();

  // Get the chosen dataset ID
  var datasetID = $("#comboDataset").val().trim();

  function displayAssociatedRF(data, status) {
    // Multiply freq by 100 to display as %
    if (data.hasOwnProperty("results")) {
      for (var i = 0; i < data["results"].length; i++) {
        data["results"][i]["relative_frequency"] = (data["results"][i]["relative_frequency"] * 100).toFixed(6);
      }
    }

    var columns = ["Concept ID 2", "Concept 2 Name", "Concept 2 Domain", "Concept Pair Count", "Relative Frequency (%)"];
    var fields = ["concept_id_2", "concept_2_name", "concept_2_domain", "concept_pair_count", "relative_frequency"];
    displayDynatable(data, columns, fields);

    // Allow user interaction again
    $.unblockUI();
  }

  if (input2.length == 0) {
    // No input specified for concept 2. Get RF between concept 1 and all concepts.
    var queryParams = {"concept_id_1": conceptID, "dataset_id": datasetID};
    var url = urlEndpoint + encodeQueryData(queryParams);
    $.get(url, displayAssociatedRF);
    lastURL = url;

    // Prevent user interaction with site while loading association data
    $.blockUI(blockUIOptions);
  } else if(setDomains.has(input2.toLowerCase())) {
    // Domain specified for concept 2. Get RF between concept 1 and all concepts in domain.
    var queryParams = {"concept_id_1": conceptID, "domain": input2, "dataset_id": datasetID};
    var url = urlEndpoint + encodeQueryData(queryParams);
    $.get(url, displayAssociatedRF);
    lastURL = url;

    // Prevent user interaction with site while loading association data
    $.blockUI(blockUIOptions);
  } else if(setConceptClasses.has(input2.toLowerCase())) {
    // Concept class specified for concept 2. Get RF between concept 1 and all concepts in class.
    var queryParams = {"concept_id_1": conceptID, "concept_class": input2, "dataset_id": datasetID};
    var url = urlEndpoint + encodeQueryData(queryParams);
    $.get(url, displayAssociatedRF);
    lastURL = url;

    // Prevent user interaction with site while loading association data
    $.blockUI(blockUIOptions);
  } else if(checkConceptID(input2)) {
    // Show single concept info for concept 2
    var urlConcept = new URL("/api/omop/concepts?", baseURL);
    urlConcept += encodeQueryData({
      q: input2,
      dataset_id: datasetID
    });
    $.get(urlConcept, displayConcept2);

    // Get RF between just concept 1 and concept 2
    var queryParams = {"concept_id_1": conceptID, "concept_id_2": input2, "dataset_id": datasetID};
    var url = urlEndpoint + encodeQueryData(queryParams);
    $.get(url, displayPairedRF);
  } else {
    $("#divPairedResults").empty();
    message(messageConcept2);
  }
}

function handleButtonSingleConcept() {
  // Clear contents for concept 2 and paired results
  $("#divConcept2Results").empty();
  $("#divPairedResults").empty();

  getConcept1();
}

function getConcept1() {
  // Get concept ID(s)
  var conceptID1 = $("#inputConcept1").val().trim();

    // Check the input
  if (!checkConceptID(conceptID1)) {
    message(messageConcept1);
    return;
  }

  // Get the chosen dataset ID
  var datasetID = $("#comboDataset").val();

  // Call concepts endpoint to get information about the concept
  var urlConcept = new URL("/api/omop/concepts?", baseURL);
  urlConcept += encodeQueryData({
    q: conceptID1,
    dataset_id: datasetID
  });
  $.get(urlConcept, displayConcept1);
}

function handleButtonPairedConcept() {
  // Clear contents for concept 2 and paired results
  $("#divConcept2Results").empty();
  $("#divPairedResults").empty();

  // Show single concept info for concept 1
  getConcept1();

  // Determine what type of analysis the user chose
  var method = $("#comboAssocMethod").val();

  switch(method) {
    case "count":
      pairedCounts();
      break;
    case "chi":
      pairedChi();
      break;
    case "oefr":
      pairedOEFR();
      break;
    case "rf":
      pairedRF();
      break;
    default:
      // Default to returning raw count
      pairedCounts();
  }
}

function message(msg) {
  var divMsg = $('<div class="alert"><span class="closebtn" onclick="this.parentElement.style.display=\'none\';">&times;</span></div>');
  divMsg.append(msg);
  $("#divMessages").append(divMsg);
}

/*********************************************************************************
* Document ready
*********************************************************************************/

$(document).ready(function() {
  // Dynamically populate dataset selector
  populateDatasetSelector();

  // Add handler for single concept button
  $("#buttonConcept1").click(handleButtonSingleConcept);

  // Add handler for paired concepts button
  $("#buttonConcept2").click(handleButtonPairedConcept);

  // Perform autocomplete after the user stops typing for 500 ms
  var inputC1 = document.querySelector('#inputConcept1');
  inputC1.onkeyup = delay(function (e) {
    conceptAutocomplete(e, inputC1)
  }, 500);
  autocomplete(inputC1);

  // Perform autocomplete after the user stops typing for 500 ms
  var inputC2 = document.querySelector('#inputConcept2')
  inputC2.onkeyup = delay(function (e) {
    conceptAutocomplete(e, inputC2)
  }, 500);
  autocomplete(inputC2);

  // Show some suggestions when the inputs for Concepts 1 and 2 receive focus and don't have content
  inputC1.onfocus = handleFocusConcept1;
  inputC2.onfocus = handleFocusConcept2;

  // Close autocomplete lists when users click in the document
  document.addEventListener("click", function (e) {
      closeAutocompleteLists(e.target);
  });
});
