package com.linkedin.thirdeye.dashboard;

import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.client.HttpClientBuilder;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.dropwizard.views.ViewBundle;

import java.io.File;
import java.util.concurrent.ExecutorService;

import org.apache.http.client.HttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.dashboard.resources.CollectionConfigResource;
import com.linkedin.thirdeye.dashboard.resources.CustomDashboardResource;
import com.linkedin.thirdeye.dashboard.resources.DashboardResource;
import com.linkedin.thirdeye.dashboard.resources.FlotTimeSeriesResource;
import com.linkedin.thirdeye.dashboard.resources.FunnelsDataProvider;
import com.linkedin.thirdeye.dashboard.resources.MetadataResource;
import com.linkedin.thirdeye.dashboard.task.ClearCachesTask;
import com.linkedin.thirdeye.dashboard.util.ConfigCache;
import com.linkedin.thirdeye.dashboard.util.DataCache;
import com.linkedin.thirdeye.dashboard.util.QueryCache;

public class ThirdEyeDashboard extends Application<ThirdEyeDashboardConfiguration> {
  private static final Logger LOG = LoggerFactory.getLogger(CustomDashboardResource.class);

  @Override
  public String getName() {
    return "thirdeye-dashboard";
  }

  @Override
  public void initialize(Bootstrap<ThirdEyeDashboardConfiguration> bootstrap) {
    bootstrap.addBundle(new ViewBundle());
    bootstrap.addBundle(new AssetsBundle("/assets/css", "/assets/css", null, "css"));
    bootstrap.addBundle(new AssetsBundle("/assets/js", "/assets/js", null, "js"));
    bootstrap.addBundle(new AssetsBundle("/assets/img", "/assets/img", null, "img"));
  }

  @Override
  public void run(ThirdEyeDashboardConfiguration config, Environment environment) throws Exception {

    LOG.error("running the Dashboard application with configs, {}", config.toString());

    final HttpClient httpClient =
        new HttpClientBuilder(environment)
            .using(config.getHttpClient())
            .build(getName());

    ExecutorService queryExecutor = environment.lifecycle().executorService("query_executor").build();

    ConfigCache configCache = new ConfigCache();
    DataCache dataCache = new DataCache(httpClient, environment.getObjectMapper());
    QueryCache queryCache = new QueryCache(httpClient, environment.getObjectMapper(), queryExecutor);

    CustomDashboardResource customDashboardResource = null;
    if (config.getCustomDashboardRoot() != null) {
      File customDashboardDir = new File(config.getCustomDashboardRoot());
      configCache.setCustomDashboardRoot(customDashboardDir);
      customDashboardResource = new CustomDashboardResource(customDashboardDir, config.getServerUri(), queryCache, dataCache, configCache);
      environment.jersey().register(customDashboardResource);
    }

    FunnelsDataProvider funnelsResource = null;
    if (config.getFunnelConfigRoot() != null) {
      funnelsResource = new FunnelsDataProvider(new File(config.getFunnelConfigRoot()), config.getServerUri(), queryCache, dataCache);
      environment.jersey().register(funnelsResource);
    }

    if (config.getCollectionConfigRoot() != null) {
      File collectionConfigDir = new File(config.getCollectionConfigRoot());
      configCache.setCollectionConfigRoot(collectionConfigDir);
      CollectionConfigResource collectionConfigResource = new CollectionConfigResource(collectionConfigDir, configCache);
      environment.jersey().register(collectionConfigResource);
    }

    environment.jersey().register(new DashboardResource(
        config.getServerUri(),
        dataCache,
        config.getFeedbackEmailAddress(),
        queryCache,
        environment.getObjectMapper(),
        customDashboardResource,
        configCache, funnelsResource));

    environment.jersey().register(new FlotTimeSeriesResource(
        config.getServerUri(),
        dataCache,
        queryCache,
        environment.getObjectMapper(),
        configCache,
        config.getAnomalyDatabaseConfig()));

    environment.jersey().register(new MetadataResource(config.getServerUri(), dataCache));

    environment.admin().addTask(new ClearCachesTask(dataCache, queryCache, configCache));
  }

  public static void main(String[] args) throws Exception {
    new ThirdEyeDashboard().run(args);
  }
}
