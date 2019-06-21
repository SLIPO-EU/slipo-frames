console.log(data);

function formatDate(timestamp) {
    var d = new Date(timestamp),
      month = '' + (d.getMonth() + 1),
      day = '' + d.getDate(),
      year = d.getFullYear();

    if (month.length < 2) month = '0' + month;
    if (day.length < 2) day = '0' + day;

    return [day, month, year].join('/');
  }

  function findStepByOutputKey(steps, outputKey) {
    for (var stepIndex = 0; stepIndex < steps.length; stepIndex++) {
      if (steps[stepIndex].outputKey === outputKey) {
        return steps[stepIndex];
      }
    }
    return null;
  }

  function stepKeyToExecution(steps, stepKey) {
    for (var stepIndex = 0; stepIndex < steps.length; stepIndex++) {
      if (steps[stepIndex].key === stepKey) {
        return steps[stepIndex];
      }
    }
    return null;
  }

  function statusToClass(execution) {
    if (execution) {
      switch (execution.status) {
        case 'COMPLETED':
          return 'completed';
        case 'RUNNING':
          return 'running';
        case 'UNKNOWN':
          return 'unknown';
        case 'FAILED':
          return 'failed';
        case 'STOPPED':
          return 'stopped';
      }
    }

    return '';
  }

  function formatTime(millis) {
    var minutes = Math.floor(millis / 60000);
    var seconds = ((millis % 60000) / 1000).toFixed(0);
    return (minutes ? minutes + " mins " : "") + (seconds < 10 ? '0' : '') + seconds + ' secs';
  }

  //Create a new directed graph
  var g = new dagreD3.graphlib.Graph();
  g.setGraph({
    nodesep: 70,
    ranksep: 50,
    marginx: 20,
    marginy: 20,
    rankdir: "LR",
  });

  // Set up states
  var steps = data.process.steps;

  var states = {};
  for (var stepIndex = 0; stepIndex < steps.length; stepIndex++) {
    var step = steps[stepIndex];
    var execution = stepKeyToExecution(data.execution.steps, step.key);
    states[step.name] = {
      description: step.name,
      step: step,
      execution: execution,
      class: statusToClass(execution),
    };
  }

  // Create the renderer
  var render = new dagreD3.render();

  // Set up an SVG group so that we can translate the final graph.
  var svg = d3.select("svg");
  svg.select("g").remove();

  var inner = svg.append("g");

  // Set up zoom support
  var zoom = d3.zoom()
    .on("zoom", function () {
      inner.attr("transform", d3.event.transform);
    });
  svg.call(zoom);

  function setup() {
    svg.select("g").remove();
    inner = svg.append("g");

    // Add states to the graph, set labels, and style
    Object.keys(states).forEach(function (state) {
      var value = states[state];

      var time = value.execution.startedOn ? (value.execution.completedOn ? value.execution.completedOn : Date.now()) - value.execution.startedOn : null;

      var html = "<div class='wrapper'>";
      html += "<div class='name'>";
      html += "<p>" + value.step.name + "</p>";
      html += "</div>";
      html += "<div class='status " + value.class + "'>";
      html += '<div>' + value.execution.status + '</div>';
      html += '<div>' + (time ? formatTime(time) : '') + '</div>';
      html += "</div>";
      html += "</div>";
      g.setNode(state, {
        labelType: "html",
        label: html,
        padding: 0,
        class: "container ",
      });
    });

    // Set up the edges
    for (var stepIndex = 0; stepIndex < steps.length; stepIndex++) {
      var step = steps[stepIndex];
      for (var inputIndex = 0; inputIndex < step.inputKeys.length; inputIndex++) {
        var input = step.inputKeys[inputIndex]
        var requiredStep = findStepByOutputKey(steps, input);
        if (requiredStep) {
          g.setEdge(requiredStep.name, step.name, { label: "" });
        }
      }
    }



    // Run the renderer. This is what draws the final graph.
    inner.call(render, g);

    // Center the graph
    var initialScale = 0.75;
    svg.call(zoom.transform, d3.zoomIdentity.translate(($('.svg').width() - g.graph().width * initialScale) / 2, 20).scale(initialScale));

    svg.attr('height', g.graph().height * initialScale + 40);

    // Set status
    var time = data.execution.startedOn ? (data.execution.completedOn ? data.execution.completedOn : Date.now()) - data.execution.startedOn : null;

    $('.status-label').html('<span>' + data.execution.status + ' ' + (time ? formatTime(time) : '') + '</span>');
    $('.status-label')[0].className = 'status-label ' + statusToClass(data.execution)
  }

  $(document).ready(function () {
    setup();
    var interval = setInterval(function () {
      states['OSM'].execution.status = 'RUNNING';
      states['OSM'].class = statusToClass(states['OSM'].execution);
      setup();

      if (data.execution.status != 'RUNNING') {
        clearInterval(interval);

      }
    }, 2000);

    setTimeout(function () {
      data.execution.completedOn = Date.now();
      data.execution.status = 'COMPLETED';
      setup();
    }, 10500);

  });


