/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.repositories.uri;

import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.url.URLBlobStore;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.util.URIPattern;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.snapshots.IndexShardRepository;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.RepositoryName;
import org.elasticsearch.repositories.RepositorySettings;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Read-only URL-based implementation of the BlobStoreRepository
 * <p>
 * This repository supports the following settings
 * <dl>
 * <dt>{@code url}</dt><dd>URL to the root of repository. This is mandatory parameter.</dd>
 * <dt>{@code concurrent_streams}</dt><dd>Number of concurrent read/write stream (per repository on each node). Defaults to 5.</dd>
 * </dl>
 */
public class URLRepository extends BlobStoreRepository {

    public final static String TYPE = "url";

    public static final Setting<List<String>> SUPPORTED_PROTOCOLS_SETTING =
        Setting.listSetting("repositories.url.supported_protocols", Arrays.asList("http", "https", "ftp", "file", "jar"),
            Function.identity(), Property.NodeScope);

    public static final Setting<List<URIPattern>> ALLOWED_URLS_SETTING =
        Setting.listSetting("repositories.url.allowed_urls", Collections.emptyList(), URIPattern::new, Property.NodeScope);

    public static final Setting<URL> URL_SETTING = new Setting<>("url", "http:", URLRepository::parseURL, Property.NodeScope);
    public static final Setting<URL> REPOSITORIES_URL_SETTING =
        new Setting<>("repositories.url.url", (s) -> s.get("repositories.uri.url", "http:"), URLRepository::parseURL,
            Property.NodeScope);

    public static final Setting<Boolean> LIST_DIRECTORIES_SETTING =
        Setting.boolSetting("list_directories", true, Property.NodeScope);
    public static final Setting<Boolean> REPOSITORIES_LIST_DIRECTORIES_SETTING =
        Setting.boolSetting("repositories.uri.list_directories", true, Property.NodeScope);

    private final List<String> supportedProtocols;

    private final URIPattern[] urlWhiteList;

    private final Environment environment;

    private final URLBlobStore blobStore;

    private final BlobPath basePath;

    private boolean listDirectories;

    /**
     * Constructs new read-only URL-based repository
     *
     * @param name                 repository name
     * @param repositorySettings   repository settings
     * @param indexShardRepository shard repository
     */
    @Inject
    public URLRepository(RepositoryName name, RepositorySettings repositorySettings, IndexShardRepository indexShardRepository, Environment environment) throws IOException {
        super(name.getName(), repositorySettings, indexShardRepository);

        if (URL_SETTING.exists(repositorySettings.settings()) == false && REPOSITORIES_URL_SETTING.exists(settings) ==  false) {
            throw new RepositoryException(name.name(), "missing url");
        }
        supportedProtocols = SUPPORTED_PROTOCOLS_SETTING.get(settings);
        urlWhiteList = ALLOWED_URLS_SETTING.get(settings).toArray(new URIPattern[]{});
        this.environment = environment;
        listDirectories = LIST_DIRECTORIES_SETTING.exists(repositorySettings.settings()) ? LIST_DIRECTORIES_SETTING.get(repositorySettings.settings()) : REPOSITORIES_LIST_DIRECTORIES_SETTING.get(settings);

        URL url = URL_SETTING.exists(repositorySettings.settings()) ? URL_SETTING.get(repositorySettings.settings()) : REPOSITORIES_URL_SETTING.get(settings);
        URL normalizedURL = checkURL(url);
        blobStore = new URLBlobStore(settings, normalizedURL);
        basePath = BlobPath.cleanPath();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected BlobStore blobStore() {
        return blobStore;
    }

    @Override
    protected BlobPath basePath() {
        return basePath;
    }

    @Override
    public List<SnapshotId> snapshots(final Predicate<String> filter) {
        if (listDirectories) {
            return super.snapshots(filter);
        } else {
            try {
                return readSnapshotList();
            } catch (IOException ex) {
                throw new RepositoryException(repositoryName, "failed to get snapshot list in repository", ex);
            }
        }
    }

    /**
     * Makes sure that the url is white listed or if it points to the local file system it matches one on of the root path in path.repo
     */
    private URL checkURL(URL url) {
        String protocol = url.getProtocol();
        if (protocol == null) {
            throw new RepositoryException(repositoryName, "unknown url protocol from URL [" + url + "]");
        }
        for (String supportedProtocol : supportedProtocols) {
            if (supportedProtocol.equals(protocol)) {
                try {
                    if (URIPattern.match(urlWhiteList, url.toURI())) {
                        // URL matches white list - no additional processing is needed
                        return url;
                    }
                } catch (URISyntaxException ex) {
                    logger.warn("cannot parse the specified url [{}]", url);
                    throw new RepositoryException(repositoryName, "cannot parse the specified url [" + url + "]");
                }
                // We didn't match white list - try to resolve against path.repo
                URL normalizedUrl = environment.resolveRepoURL(url);
                if (normalizedUrl == null) {
                    logger.warn("The specified url [{}] doesn't start with any repository paths specified by the path.repo setting or by {} setting: [{}] ", url, ALLOWED_URLS_SETTING.getKey(), environment.repoFiles());
                    throw new RepositoryException(repositoryName, "file url [" + url + "] doesn't match any of the locations specified by path.repo or " + ALLOWED_URLS_SETTING.getKey());
                }
                return normalizedUrl;
            }
        }
        throw new RepositoryException(repositoryName, "unsupported url protocol [" + protocol + "] from URL [" + url + "]");
    }

    @Override
    public boolean readOnly() {
        return true;
    }

    private static URL parseURL(String s) {
        try {
            return new URL(s);
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("Unable to parse URL repository setting", e);
        }
    }
}
