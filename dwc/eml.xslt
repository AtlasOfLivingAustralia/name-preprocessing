<!--
  ~ Copyright (c) 2021.  Atlas of Living Australia
  ~  All Rights Reserved.
  ~
  ~  The contents of this file are subject to the Mozilla Public
  ~  License Version 1.1 (the "License"); you may not use this file
  ~  except in compliance with the License. You may obtain a copy of
  ~  the License at http://www.mozilla.org/MPL/
  ~
  ~  Software distributed under the License is distributed on an "AS  IS" basis,
  ~  WITHOUT WARRANTY OF ANY KIND, either express or
  ~  implied. See the License for the specific language governing
  ~  rights and limitations under the License.
  -->

<xsl:stylesheet version="1.0"
                xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:eml="eml://ecoinformatics.org/eml-2.1.1"
>
    <xsl:param name="created"/>
    <xsl:param name="date" select="substring-before($created, 'T')"/>
    <xsl:param name="label" select="translate($date, '0123456789-', '0123456789')"/>
    <xsl:param name="year" select="substring-before($created, '-')"/>

    <xsl:output method="xml" encoding="UTF-8" indent="yes"/>
    <xsl:strip-space elements="*"/>
    <xsl:template match="/metadata">
        <eml:eml>
            <dataset>
                <xsl:if test="@code">
                    <alternativeIdentifier><xsl:value-of select="@code"/>-<xsl:value-of select="$label"/>
                    </alternativeIdentifier>
                </xsl:if>
                <xsl:if test="title">
                    <title xml:lang="en"><xsl:value-of select="title"/></title>
                </xsl:if>
                <xsl:if test="publisher">
                    <creator><xsl:apply-templates select="publisher/organization"/></creator>
                </xsl:if>
                <xsl:for-each select="organization">
                    <creator><xsl:apply-templates select="."/></creator>
                </xsl:for-each>
                <xsl:if test="publisher">
                    <metadataProvider>
                        <xsl:apply-templates select="publisher/organization"/>
                    </metadataProvider>
                </xsl:if>
                <pubDate>
                    <xsl:value-of select="$date"/>
                </pubDate>
                <language>eng</language>
                <abstract>
                    <xsl:for-each select="description">
                        <para><xsl:copy-of select="node()"/></para>
                    </xsl:for-each>
                    <para>Source data retrieved on <xsl:value-of select="@date"/></para>
                </abstract>
                <intellectualRights>
                    <xsl:if test="publisher">
                      <para>Copyright <xsl:value-of select="$year"/>, <xsl:value-of select="publisher/organization/organizationName"/></para>
                    </xsl:if>
                    <xsl:if test="organization/organizationName">
                        <xsl:for-each select="organization/organizationName">
                            <para>Copyright <xsl:value-of select="$year"/>, <xsl:value-of select="."/></para>
                        </xsl:for-each>
                    </xsl:if>
                    <xsl:if test="license">
                        <para><xsl:copy-of select="license/node()"/></para>
                    </xsl:if>
                </intellectualRights>
                <xsl:if test="publisher">
                    <distribution scope="document">
                        <online>
                            <url function="information"><xsl:value-of select="publisher/organization/onlineUrl"/></url>
                        </online>
                    </distribution>
                </xsl:if>
                <coverage>
                    <xsl:if test="geographicCoverage">
                        <xsl:copy-of select="geographicCoverage"/>
                    </xsl:if>
                    <xsl:if test="taxonomicCoverage">
                        <xsl:copy-of select="taxonomicCoverage"/>
                    </xsl:if>
                </coverage>
                <xsl:if test="publisher">
                    <contact>
                        <xsl:apply-templates select="publisher/organization"/>
                    </contact>
                </xsl:if>
            </dataset>
            <additionalMetadata>
                <metadata>
                    <gbif>
                        <dateStamp><xsl:value-of select="$created"/></dateStamp>
                        <citation>
                            <xsl:attribute name="identifier">
                                <xsl:value-of select="concat(@code, '-', $label)"/>
                            </xsl:attribute>
                            <xsl:value-of select="citation"/><xsl:text> </xsl:text><xsl:value-of select="@date"/>
                        </citation>
                    </gbif>
                </metadata>
            </additionalMetadata>
        </eml:eml>
    </xsl:template>

    <xsl:template match="organization">
        <xsl:copy-of select="*"/>
    </xsl:template>
</xsl:stylesheet>
