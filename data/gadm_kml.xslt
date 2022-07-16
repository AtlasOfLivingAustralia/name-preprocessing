<?xml version="1.0"?>
<!--
  ~ Copyright (c) 2021-2022.  Atlas of Living Australia
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

<xsl:stylesheet
 xmlns:xsl=
    "http://www.w3.org/1999/XSL/Transform"
 xmlns:kml="http://www.opengis.net/kml/2.2"
 version="1.0"
 >
 <xsl:output method="xml" encoding="UTF-8"/>

 <xsl:template match="/">
  <places>
  <xsl:apply-templates/>
  </places>
 </xsl:template>

 <xsl:template match="kml:kml"><xsl:apply-templates/></xsl:template>

 <xsl:template match="kml:Folder"><xsl:apply-templates/></xsl:template>

 <xsl:template match="kml:Placemark">
  <place>
   <xsl:attribute name="id"><xsl:value-of select="kml:ExtendedData/kml:SchemaData/kml:SimpleData[@name = 'ENGTYPE_1']"/></xsl:attribute>
   <xsl:attribute name="type"><xsl:value-of select="kml:ExtendedData/kml:SchemaData/kml:SimpleData[@name = 'VARNAME_1']"/></xsl:attribute>
   <xsl:value-of select="kml:ExtendedData/kml:SchemaData/kml:SimpleData[@name = 'COUNTRY']"/>
  </place>
 </xsl:template>

</xsl:stylesheet>
