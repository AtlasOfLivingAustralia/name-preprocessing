<?xml version="1.0"?>
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

<xsl:stylesheet
 xmlns:xsl=
    "http://www.w3.org/1999/XSL/Transform" 
 version="1.0"
 >
 <xsl:output method="html" encoding="UTF-8"/>  
 
 <xsl:param name="rank" select="'species'"/>
 
 <xsl:template match="/"><span><xsl:attribute name="class">scientific-name rank-<xsl:value-of select="$rank"/></xsl:attribute><xsl:apply-templates/></span></xsl:template>
 
 <xsl:template match="cultivar"><xsl:apply-templates/></xsl:template>

 <xsl:template match="scientific"><xsl:apply-templates/></xsl:template>
 
 <xsl:template match="name"><xsl:apply-templates/></xsl:template>
 
 <xsl:template match="authors"><xsl:apply-templates/></xsl:template>
 
 <xsl:template match="element"><span class="name"><xsl:apply-templates/></span></xsl:template>
 
 <xsl:template match="i"><xsl:apply-templates/></xsl:template>

 <xsl:template match="rank"><span class="rank"><xsl:apply-templates/></span></xsl:template>
  
 <xsl:template match="hybrid"><span class="hybrid"><xsl:apply-templates/></span></xsl:template>
  
 <xsl:template match="manuscript"><span class="manuscript"><xsl:apply-templates/></span></xsl:template>
 
 <xsl:template match="author"><span class="author"><xsl:apply-templates/></span></xsl:template>
 
 <xsl:template match="ex"><span class="author ex-author"><xsl:apply-templates/></span></xsl:template>

 <xsl:template match="base"><span class="author base-author"><xsl:apply-templates/></span></xsl:template>

 <xsl:template match="ex-base"><span class="author ex-author base-author"><xsl:apply-templates/></span></xsl:template>
 <xsl:template match="text()"><xsl:value-of select="."/></xsl:template>
 
</xsl:stylesheet>
