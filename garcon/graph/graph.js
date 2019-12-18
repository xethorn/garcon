// This is adapted from https://bl.ocks.org/mbostock/2675ff61ea5e063ede2b5d63c08020c7

const getDescription = (d) => Object.keys(d)
    .map((k)=>k+": "+d[k]+"<br>")
    .join("");

const getColorFromFailure = (ratio) => {
    const red = ratio > 0.1 ? 255 : Math.floor((255/0.1)*ratio);
    let green = 0;
    if(ratio<0.1) {
        green = 255
    } else if (ratio <0.2) {
        green = Math.floor((255/0.2)*(0.2-ratio))
    } else {
        green = 0;
    }
    return `rgb(${red},${green},0)`;
}


var svg = d3.select("svg"),
    width = +svg.attr("width"),
    height = +svg.attr("height");

var simulation = d3.forceSimulation()
    .force("link", d3.forceLink().id(function (d) {
        return d.id;
    }).distance(100))
    .force("charge", d3.forceManyBody().strength(-300))
    .force("center", d3.forceCenter(500, 500));

d3.json("graph/graph.json", function (error, graph) {
    if (error) throw error;

    svg.append("svg:defs").append("svg:marker")
    .attr("id", "triangle")
    .attr("refX", 100)
    .attr("refY", 5)
    .attr("markerWidth", 30)
    .attr("markerHeight", 30)
    .attr("markerUnits","userSpaceOnUse")
    .attr("orient", "auto")
    .append("path")
    .attr("d", "M 0 0 10 5 0 10")
    .style("fill", "black");

    var label = svg.append("g")
    .attr("class", "labels")
    .append("foreignObject")
    .attr("width", "100%")
    .attr("height", "100%");

    var link = svg.append("g")
        .attr("class", "links")
        .selectAll("line")
        .data(graph.links)
        .enter().append("line")
        .attr("marker-end", "url(#triangle)");

    var node = svg.append("g")
        .attr("class", "nodes")
        .selectAll("circle")
        .data(graph.nodes)
        .enter().append("circle")
        .attr("r", (d)=>{return Math.ceil(Math.sqrt(d.avg_duration+10 || 10))})
        .attr("fill", (d)=>{
            return getColorFromFailure(d.failure_n/(d.failure_n+d.success_n+1))
        })
        .call(d3.drag()
            .on("start", dragstarted)
            .on("drag", dragged)
            .on("end", dragended));
    


    var labelDiv = label
    .selectAll("div")
    .data(graph.nodes)
    .enter()
    .append("xhtml:div")
    .html(function (d) { return d.name; })
    .style("position", "absolute")
    .style("text-anchor", "middle")
    .style("fill", "#555")
    .style("font-family", "Arial")
    .style("font-size", 20)
    .style("z-index", 10)
    .style("cursor", "pointer")
    .on("click", function(d) {
        let element = d3
        .select(this);
        if(element.html()===d.name) {
            element
            .html(function (d) { return getDescription(d) });
        } else {
            element
            .html(function (d) { return d.name; });
        } 
    });

    node.append("title")
        .text(function (d) {
            return d.id;
        });

    simulation
        .nodes(graph.nodes)
        .on("tick", ticked);

    simulation.force("link")
        .links(graph.links);

    function ticked() {
        link
            .attr("x1", function (d) {
                return d.source.x;
            })
            .attr("y1", function (d) {
                return d.source.y;
            })
            .attr("x2", function (d) {
                return d.target.x;
            })
            .attr("y2", function (d) {
                return d.target.y;
            });
        node
            .attr("cx", function (d) {
                return d.x;
            })
            .attr("cy", function (d) {
                return d.y;
            });
        labelDiv
            .style("left", function(d){return (d.x + 20) + "px"; })
            .style("top", function(d){return (d.y + 10) + "px"; });
    }
});

function dragstarted(d) {
    if (!d3.event.active) simulation.alphaTarget(0.3).restart();
    d.fx = d.x;
    d.fy = d.y;
}

function dragged(d) {
    d.fx = d3.event.x;
    d.fy = d3.event.y;
}

function dragended(d) {
    if (!d3.event.active) simulation.alphaTarget(0);
    d.fx = null;
    d.fy = null;
}