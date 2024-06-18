google.charts.load('current', {'packages':['corechart']});


function submitForm() {
  // Hide the "OR" section and the upload form container
  document.getElementById('orSection').style.display = 'none';
  document.getElementById('uploadFormContainer').style.display = 'none';

  // Hide the API form and show the loading container
  document.getElementById('apiForm').style.display = 'none';
  document.getElementById('loadingContainer').style.display = 'block';

  // Enable the "Download Report" link
  document.getElementById('downloadReportLink').removeAttribute('disabled');

  // Delay the showing of the dashboard for 5 seconds
  setTimeout(() => {
    // Load the dashboard after a delay
    document.getElementById('dashboardContainer').style.display = 'block';
    drawCharts();
    setInterval(updateCharts, 50000); // Update every 5 seconds

    // Hide the loading container after processing
    document.getElementById('loadingContainer').style.display = 'none';
  }, 50000);

  // Get the form data
  var formData = new FormData(document.getElementById('apiForm'));

  // Send a POST request to the specified URL
  fetch('/loadingdata', {
    method: 'POST',
    body: formData
  })
  .then(response => response.json())
  .then(data => {
    // Process the data if needed
  })
  .catch(error => {
    console.error('Error:', error);
    // Handle errors if needed
  });
}

function drawCharts() {
  drawBarChart();
  drawLineChart();
  drawDoughnutChart();
  drawGroupedBarChart()
  drawHistogramChart();
  drawScatterChart();
}

function updateCharts() {
  updateBarChart();
  updateLineChart();
  updateDoughnutChart();
  updateGroupedBarChart();
  updateHistogramChart();
  updateScatterChart();

}

function drawBarChart() {
  fetch('/get_payment_method_data')
      .then(response => response.json())
      .then(data => {
         
          var predefinedColors = ['#E0B0FF', '#DF73FF', '#DF00FF', '#FF00FF'];

          var chartData = google.visualization.arrayToDataTable([
              ['Payment Method', 'Customer Count', { role: 'style' }],
              ...data.map((row, index) => [row.payment_method, row.customer_count, predefinedColors[index % predefinedColors.length]])
          ]);

          var view = new google.visualization.DataView(chartData);
          view.setColumns([0, 1,
              { calc: 'stringify',
                  sourceColumn: 1,
                  type: 'string',
                  role: 'annotation' },
              2]);

          var options = {
              title: 'Payment Method Counts',
              legend: { position: 'none' },
              bar: { groupWidth: '95%' },
              hAxis: {
                  title: 'Payment Method',
                  titleTextStyle: {
                      bold: true,
                      italic: false
                  }
              }
          };

          var chart = new google.visualization.BarChart(document.getElementById('barChart'));
          chart.draw(view, options);
      })
      .catch(error => {
          console.error('Error:', error);
          
      });
}

function updateBarChart() {
  
  drawBarChart();
}

function drawLineChart() {
  fetch('/get_product_performance_data')
    .then(response => response.json())
    .then(data => {
      var chartData = new google.visualization.DataTable();
      chartData.addColumn('string', 'Date');
      chartData.addColumn('number', 'Quantity Sold');
      chartData.addColumn('number', 'Total Sales Amount');

      for (var i = 0; i < data.dates.length; i++) {
        chartData.addRow([data.dates[i], data.quantitySold[i], data.totalSalesAmount[i]]);
      }

      var options = {
        title: 'Product Performance - Line Chart',
        legend: { position: 'top' },
        colors: ['orange', 'red'], 
        curveType: 'function',
        series: {
          0: {
            lineWidth: 3,
            pointSize: 8
          },
          1: {
            lineWidth: 3,
            pointSize: 8
          }
         
        }
      };

      var chart = new google.visualization.LineChart(document.getElementById('lineChart'));
      chart.draw(chartData, options);
      setInterval(function () {
        updateLineChart(chart, options);
      }, 5000);
    })
    .catch(error => {
      console.error('Error:', error);
     
    });
}

function updateLineChart() {
  fetch('/get_product_performance_data')
    .then(response => response.json())
    .then(data => {
      var updatedChartData = new google.visualization.DataTable();
      updatedChartData.addColumn('string', 'Date');
      updatedChartData.addColumn('number', 'Quantity Sold');
      updatedChartData.addColumn('number', 'Total Sales Amount');

      for (var i = 0; i < data.dates.length; i++) {
        updatedChartData.addRow([data.dates[i], data.quantitySold[i], data.totalSalesAmount[i]]);
      }

      var options = {
        title: 'Product Performance - Line Chart',
        legend: { position: 'top' },
        colors: ['orange', 'red'], 
        curveType: 'function',
        series: {
          0: {
            lineWidth: 3,
            pointSize: 8
          },
          1: {
            lineWidth: 3,
            pointSize: 8
          }
          
        }
      };

      var chart = new google.visualization.LineChart(document.getElementById('lineChart'));
      chart.draw(updatedChartData, options);
    })
    .catch(error => {
      console.error('Error:', error);
      
    });
}


function drawDoughnutChart() {
  
  fetch('/get_gender_data')
    .then(response => response.json())
    .then(data => {
      
      var chartData = [['Gender', 'Count']];
      for (const [gender, count] of Object.entries(data)) {
        chartData.push([gender, count]);
      }

      var chartDataTable = google.visualization.arrayToDataTable(chartData);

      var options = {
        title: 'Gender Distribution',
        pieHole: 0.4,
        chartArea: { width: '110%', height: '80%' },
        legend: { position: 'bottom' },
        colors:['#40E0D0', '#B2FFFF']
      };

      var chart = new google.visualization.PieChart(document.getElementById('doughnutChart'));
      chart.draw(chartDataTable, options);

     
      setInterval(function () {
        updateDoughnutChart(chart, options);
      }, 5000);
    })
    .catch(error => {
      console.error('Error:', error);
      
    });
}

function updateDoughnutChart() {
  
  fetch('/get_gender_data')
    .then(response => response.json())
    .then(data => {
     
      var chartData = [['Gender', 'Count']];
      for (const [gender, count] of Object.entries(data)) {
        chartData.push([gender, count]);
      }

      var updatedChartData = google.visualization.arrayToDataTable(chartData);

      var options = {
        title: 'Gender Distribution',
        pieHole: 0.4,
        chartArea: { width: '110%', height: '80%' },
        legend: { position: 'bottom' },
        colors:['#40E0D0', '#B2FFFF']
      };

      var chart = new google.visualization.PieChart(document.getElementById('doughnutChart'));
      chart.draw(updatedChartData, options);
    })
    .catch(error => {
      console.error('Error:', error);
     
    });
}

function drawHistogramChart() {
  fetch('/get_monetary_data')
    .then(response => response.json())
    .then(data => {
      console.log('Data:', data);

      // Create the data table
      var dataTable = new google.visualization.DataTable();
      dataTable.addColumn('number', 'Monetary Value');

      // Add rows to the data table
      data.forEach(item => {
        dataTable.addRow([item.monetary]);
      });

      // Draw the histogram chart
      drawHistogram(dataTable);

      // Set the interval to update the chart every 5000 milliseconds (5 seconds)
      setInterval(function () {
        updateHistogramChart();
      }, 5000);
    })
    .catch(error => console.error('Error fetching data:', error));

  // Function to draw the histogram chart
  function drawHistogram(dataTable) {
    var options = {
      title: 'Monetary Value Distribution',
      legend: { position: 'none' },
      colors: ['#FF00FF'],
      histogram: { bucketSize: 50 },
      bar: { groupWidth: '130%' }  // Adjust bucket size as needed
    };

    var chart = new google.visualization.Histogram(document.getElementById('histogramChart'));
    chart.draw(dataTable, options);
  }

  // Function to update the histogram chart
  function updateHistogramChart() {
    fetch('/get_monetary_data')
      .then(response => response.json())
      .then(data => {
        console.log('Updated Data:', data);

        // Create a new data table
        var updatedDataTable = new google.visualization.DataTable();
        updatedDataTable.addColumn('number', 'Monetary Value');

        // Add rows to the updated data table
        data.forEach(item => {
          updatedDataTable.addRow([item.monetary]);
        });

        // Redraw the histogram chart with the updated data table
        drawHistogram(updatedDataTable);
      })
      .catch(error => console.error('Error fetching data:', error));
  }
}
function drawScatterChart() {
  fetch('/get_product_profitability')
    .then(response => response.json())
    .then(data => {
      const chartData = new google.visualization.DataTable();
      chartData.addColumn('number', 'Total_sales_value');
      chartData.addColumn('number', 'Total_discount_value');

      for (let i = 0; i < data.length; i++) {
        chartData.addRow([data[i].total_sales_amount, data[i].total_discount_amount]);
      }

      const options = {
        title: 'Product Profitability - Dual-Y Scatter Plot',
        legend: { position: 'bottom' },
        colors: ['#FF2400', '#00FF00'],
        pointSize: 10,
        hAxis: {
          title: 'Total Sales Value'
          
        },
        vAxes: {
          0: { title: 'Total Discount Value' }
          
        },

        trendlines: {
          0: { type: 'linear', color: '#0000FF' }
        }
      };

      const chart = new google.visualization.ScatterChart(document.getElementById('scatterChart'));
      chart.draw(chartData, options);

      setInterval(function () {
        updateScatterChart(chart, options);
      }, 5000);
    })
    .catch(error => {
      console.error('Error:', error);
      
    });
}

function updateScatterChart(chart, options) {
  fetch('/get_product_profitability')
    .then(response => response.json())
    .then(data => {
      const updatedData = new google.visualization.DataTable();
      updatedData.addColumn('number', 'Total_sales_value');
      updatedData.addColumn('number', 'Total_discount_value');

      for (let i = 0; i < data.length; i++) {
        updatedData.addRow([data[i].total_sales_amount, data[i].total_discount_amount]);
      }

      chart.draw(updatedData, options);
    })
    .catch(error => {
      console.error('Error:', error);
      // Handle errors if needed
    });
}

function shuffleArray(array) {
  for (let i = array.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [array[i], array[j]] = [array[j], array[i]];
  }
}

function shuffleArray(array) {
  for (let i = array.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [array[i], array[j]] = [array[j], array[i]];
  }
}

function drawGroupedBarChart() {
  fetch('/get_location_performance_data')
    .then(response => response.json())
    .then(data => {
      
      shuffleArray(data.location_data);

      const selectedLocations = data.location_data.slice(0, 5);

      var dataTable = new google.visualization.DataTable();
      dataTable.addColumn('string', 'Location');
      dataTable.addColumn('number', 'Total Sales Amount');
      dataTable.addColumn('number', 'Total Quantity Sold');

      selectedLocations.forEach(item => {
        dataTable.addRow([item.location_store, item.total_sales_amount, item.total_quantity_sold]);
      });

      var options = {
        title: 'Grouped Bar Chart - Location Performance',
        colors: ['lightblue', 'lightgreen'], 
        chartArea: { width: '80%', height: '70%' },
        legend: { position: 'top', maxLines: 3 },
        bar: { groupWidth: '75%' }, 
        isStacked: false, 
        vAxis: { scaleType: 'log' }, 
      };

      var chart = new google.visualization.ColumnChart(document.getElementById('groupedBarChart'));
      chart.draw(dataTable, options);

      // Set the interval to update the chart every 5000 milliseconds (5 seconds)
      setInterval(function () {
        updateGroupedBarChart(chart, options);
      }, 5000);
    })
    .catch(error => console.error('Error fetching data:', error));
}

function updateGroupedBarChart(chart, options) {

  fetch('/get_location_performance_data')
    .then(response => response.json())
    .then(data => {
      
      // Check if the fetched data is valid
      if (data.location_data && data.location_data.length >= 5) {
        shuffleArray(data.location_data);

        const selectedLocations = data.location_data.slice(0, 5);

        var dataTable = new google.visualization.DataTable();
        dataTable.addColumn('string', 'Location');
        dataTable.addColumn('number', 'Total Sales Amount');
        dataTable.addColumn('number', 'Total Quantity Sold');

        selectedLocations.forEach(item => {
          dataTable.addRow([item.location_store, item.total_sales_amount, item.total_quantity_sold]);
        });

        chart.draw(dataTable, options);
      } else {
        console.error('Invalid data received:', data);
      }
    })
    .catch(error => console.error('Error fetching data:', error));
}

async function fetchDataAndUpdateDashboard() {
  const flaskEndpoint = '/get_dashboard_data';

  try {
    const response = await fetch(flaskEndpoint);
    if (!response.ok) {
      throw new Error(`HTTP error! Status: ${response.status}`);
    }

    const data = await response.json();

    // Assuming your data structure matches the expected format
    const totalSales = data.total_sales;
    const totalRevenue = data.revenue;
    const averageSales = data.avg_sales;
    const totalInventory = data.total_inventory_value;

    // Update total sales card
    document.getElementById('totalSales').textContent = `$${totalSales}`;

    // Update total revenue card
    document.getElementById('totalRevenue').textContent = `$${totalRevenue}`;

    // Update average sales card
    document.getElementById('averageSales').textContent = `$${averageSales}`;

    // Update total inventory value card
    document.getElementById('totalInventory').textContent = `$${totalInventory}`;
  } catch (error) {
    console.error('Error fetching data:', error);
  }
}

// Function to update the dashboard periodically
function updateDashboard() {
  fetchDataAndUpdateDashboard();

  // Update the dashboard every 5 seconds
  setInterval(() => {
    fetchDataAndUpdateDashboard();
  }, 5000);
}

// Call the function to update the dashboard when the page loads
updateDashboard();

function fetchInventoryTurnoverData() {
  fetch('/get_inventory_turnover_data')
    .then(response => response.json())
    .then(data => {
      var tableBody = document.getElementById('inventoryTableBody');
      tableBody.innerHTML = '';  // Clear existing rows

      for (var i = 0; i < data.product_name.length; i++) {
        var newRow = tableBody.insertRow(tableBody.rows.length);
        var cell1 = newRow.insertCell(0);
        var cell2 = newRow.insertCell(1);

        cell1.innerHTML = data.product_name[i];
        cell2.innerHTML = data.inventory_turnover[i];
      }
    })
    .catch(error => {
      console.error('Error fetching inventory turnover data:', error);
    });
}

// Call the function to fetch and update inventory turnover data
fetchInventoryTurnoverData();

// Update the inventory turnover data every 5 seconds
setInterval(fetchInventoryTurnoverData, 5000);


document.getElementById('barChart').style.width = '400px';
document.getElementById('barChart').style.height = '600px';
document.getElementById('doughnutChart').style.width = '400px';
document.getElementById('doughnutChart').style.height = '350px';

document.getElementById('groupedBarChart').style.height = '350px';
document.getElementById('histogramChart').style.width = '410px';


