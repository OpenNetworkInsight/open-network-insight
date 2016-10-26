var React = require('react');

var SpotConstants = require('../../js/constants/SpotConstants');
var SpotActions = require('../../js/actions/SpotActions');
var EdInActions = require('../../js/actions/EdInActions');
var StoryboardActions = require('../../js/actions/StoryboardActions');
var SpotUtils = require('../../js/utils/SpotUtils');

// Build and Render Toolbar
var FilterInput = require('./components/FilterInput.react');
var DateInput = require('../../js/components/DateInput.react');

function resetFilterAndReload()
{
  EdInActions.setFilter('');
  StoryboardActions.reloadComments();
};

React.render(
  (
    <form className="form-inline">
      <div className="form-group">
        <label htmlFor="ip_filter" className="control-label">IP:</label>
        <div className="input-group input-group-xs">
          <FilterInput id="ip_filter" />
          <div className="input-group-btn">
            <button className="btn btn-primary" type="button" id="btn_searchIp" title="Enter an IP Address and click the search button to filter the results." onClick={StoryboardActions.reloadComments}>
              <span className="glyphicon glyphicon-search" aria-hidden="true"></span>
            </button>
          </div>
        </div>
      </div>
      <div className="form-group">
        <label htmlFor="dataDatePicker" className="control-label">Data Date:</label>
        <div className="input-group input-group-xs">
          <DateInput id="dataDatePicker" onChange={resetFilterAndReload} />
          <div className="input-group-btn">
            <button className="btn btn-default" type="button" title="Reset filter" id="reset_ip_filter" onClick={resetFilterAndReload}>
              <span className="glyphicon glyphicon-repeat" aria-hidden="true"></span>
            </button>
          </div>
        </div>
      </div>
    </form>
  ),
  document.getElementById('nav_form')
);

// Build and Render Edge Investigation's panels
var PanelRow = require('../../js/components/PanelRow.react');
var Panel = require('../../js/components/Panel.react');

var ExecutiveThreatBriefingPanel = require('../../js/components/ExecutiveThreatBriefingPanel.react');
var IncidentProgressionPanel = require('./components/IncidentProgressionPanel.react');
var ImpactAnalysis = require('./components/ImpactAnalysisPanel.react');
var MapView = require('./components/GlobeViewPanel.react');
var TimelinePanel = require('./components/TimelinePanel.react');

var CommentsStore = require('./stores/CommentsStore');

React.render(
  <div id="spot-content" className="storyboard">
  <PanelRow>
      <Panel title={SpotConstants.COMMENTS_PANEL} expandable className="col-md-6" >
          <ExecutiveThreatBriefingPanel store={CommentsStore} />
      </Panel>
      <Panel title={SpotConstants.INCIDENT_PANEL} expandable container className="col-md-6">
          <IncidentProgressionPanel className="dendrogram" />
      </Panel>
    </PanelRow>
    <PanelRow>
      <Panel title={SpotConstants.IMPACT_ANALYSIS_PANEL} expandable container className="col-md-4 sb_impact">
        <ImpactAnalysis />
      </Panel>
      <Panel title={SpotConstants.GLOBE_VIEW_PANEL} expandable container className="col-md-4 sb_globe_view">
          <MapView />
      </Panel>
      <Panel title={SpotConstants.TIMELINE_PANEL} expandable className="col-md-4 timeline" >
          <TimelinePanel>
            <div className="event-drops-wrapper" />
          </TimelinePanel>
      </Panel>
    </PanelRow>
  </div>,
document.getElementById('spot-content-wrapper')
);


var initial_filter = SpotUtils.getCurrentFilter();

// Set search criteria
SpotActions.setDate(SpotUtils.getCurrentDate());
initial_filter && StoryboardActions.setFilter(initial_filter);

// Load data
StoryboardActions.reloadComments();

// Create a hook to allow notebook iframe to reloadComments
window.iframeReloadHook = StoryboardActions.reloadComments;
